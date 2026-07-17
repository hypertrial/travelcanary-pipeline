from __future__ import annotations

import contextlib
import subprocess
import time
from queue import Empty, Queue
from threading import Thread
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource

from travelcanary_pipeline.orchestration.config import DbtBuildConfig
from travelcanary_pipeline.storage.duckdb.atomic_build import atomic_dbt_warehouse
from travelcanary_pipeline.storage.duckdb.connection import active_duckdb_path


def _cleanup_dbt_adapter(invocation: Any) -> None:
    adapter = getattr(invocation, "adapter", None)
    cleanup_connections = getattr(adapter, "cleanup_connections", None)
    if callable(cleanup_connections):
        with contextlib.suppress(Exception):
            cleanup_connections()
    connections = getattr(adapter, "connections", None)
    cleanup_all = getattr(connections, "cleanup_all", None)
    if callable(cleanup_all):
        with contextlib.suppress(Exception):
            cleanup_all()


def _stop_dbt_process(
    invocation: Any, *, asset_name: str, context: AssetExecutionContext
) -> float:
    process = invocation.process
    grace_seconds = float(getattr(invocation, "termination_timeout_seconds", 25))
    if process.poll() is not None:
        return grace_seconds
    try:
        process.terminate()
    except ProcessLookupError:
        process.wait(timeout=grace_seconds)
        return grace_seconds
    try:
        process.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        context.log.warning(
            "%s dbt process did not terminate within %.1f seconds; killing",
            asset_name,
            grace_seconds,
        )
        process.kill()
        process.wait(timeout=grace_seconds)
    return grace_seconds


def stream_dbt_build(
    *,
    asset_name: str,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtBuildConfig,
):
    primary_path = active_duckdb_path()
    with atomic_dbt_warehouse(primary_path):
        yield from _stream_candidate_build(
            asset_name=asset_name,
            context=context,
            dbt=dbt,
            config=config,
        )


def _stream_candidate_build(
    *,
    asset_name: str,
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    config: DbtBuildConfig,
):
    build_args = ["build"]
    if config.full_refresh:
        build_args.append("--full-refresh")
    invocation = dbt.cli(build_args, context=context)
    sentinel = object()
    event_queue: Queue[Any] = Queue()
    producer_error: list[Exception] = []

    def _producer() -> None:
        try:
            for event in invocation.stream():
                event_queue.put(event)
        except Exception as exc:  # pragma: no cover
            producer_error.append(exc)
        finally:
            event_queue.put(sentinel)

    producer = Thread(target=_producer, daemon=True)
    producer.start()

    try:
        events_emitted = 0
        last_progress_at = time.monotonic()
        last_log_at = last_progress_at
        soft_timeout_warned = False
        while True:
            try:
                item = event_queue.get(timeout=config.progress_poll_seconds)
            except Empty:
                now = time.monotonic()
                idle_seconds = now - last_progress_at
                if idle_seconds >= config.no_progress_hard_timeout_seconds:
                    context.log.error(
                        "%s dbt build made no progress for %.1f seconds; terminating",
                        asset_name,
                        idle_seconds,
                    )
                    grace_seconds = _stop_dbt_process(
                        invocation, asset_name=asset_name, context=context
                    )
                    producer.join(timeout=grace_seconds)
                    raise RuntimeError(
                        f"{asset_name} dbt build exceeded no-progress hard timeout "
                        f"({config.no_progress_hard_timeout_seconds}s)"
                    )
                if (
                    not soft_timeout_warned
                    and idle_seconds >= config.no_progress_soft_timeout_seconds
                ):
                    context.log.warning(
                        "%s dbt build has made no progress for %.1f seconds",
                        asset_name,
                        idle_seconds,
                    )
                    soft_timeout_warned = True
                if now - last_log_at >= config.progress_log_interval_seconds:
                    context.log.info(
                        "%s dbt build waiting; events_emitted=%s idle_seconds=%.1f",
                        asset_name,
                        events_emitted,
                        idle_seconds,
                    )
                    last_log_at = now
                continue
            if item is sentinel:
                break
            events_emitted += 1
            now = time.monotonic()
            last_progress_at = now
            soft_timeout_warned = False
            if (
                events_emitted % config.progress_log_interval_events == 0
                or now - last_log_at >= config.progress_log_interval_seconds
            ):
                context.log.info(
                    "%s dbt build progress; events_emitted=%s",
                    asset_name,
                    events_emitted,
                )
                last_log_at = now
            yield item

        producer.join(timeout=config.progress_poll_seconds * 2)
        if producer_error:
            raise producer_error[0]

        returncode = getattr(invocation.process, "returncode", None)
        if returncode not in (None, 0):
            raise RuntimeError(
                f"{asset_name} dbt build failed with exit code {returncode}"
            )
    finally:
        _cleanup_dbt_adapter(invocation)


__all__ = ["stream_dbt_build"]
