from __future__ import annotations

import subprocess
from contextlib import contextmanager
from queue import Queue
from threading import Thread
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from travelcanary_pipeline.orchestration.config import DbtBuildConfig
from travelcanary_pipeline.orchestration.dbt_build import (
    _cleanup_dbt_adapter,
    _stop_dbt_process,
    stream_dbt_build,
)


class _Invocation:
    def __init__(self, events=(), returncode=0):
        self._events = list(events)
        self.process = Mock(returncode=returncode)

    def stream(self):
        yield from self._events


@pytest.fixture(autouse=True)
def _candidate_warehouse(monkeypatch):
    @contextmanager
    def candidate(_path):
        yield _path

    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.dbt_build.atomic_dbt_warehouse",
        candidate,
    )
    monkeypatch.setattr(
        "travelcanary_pipeline.orchestration.dbt_build.active_duckdb_path",
        lambda: "/tmp/test.duckdb",
    )


def test_stream_dbt_build_yields_events_and_full_refresh(monkeypatch):
    invocation = _Invocation(events=["one", "two"])
    dbt = Mock()
    dbt.cli.return_value = invocation

    events = list(
        stream_dbt_build(
            asset_name="dbt",
            context=Mock(),
            dbt=dbt,
            config=DbtBuildConfig(full_refresh=True, progress_poll_seconds=1),
        )
    )

    dbt.cli.assert_called_once()
    assert dbt.cli.call_args.args[0] == ["build", "--full-refresh"]
    assert events == ["one", "two"]


def test_stream_dbt_build_ignores_empty_poll_and_raises_on_returncode(monkeypatch):

    class EmptyOnceQueue(Queue):
        missed = False

        def get(self, *args, **kwargs):
            if not self.missed:
                self.missed = True
                from queue import Empty

                raise Empty
            return super().get(*args, **kwargs)

    dbt = Mock()
    dbt.cli.return_value = _Invocation(returncode=2)

    with (
        patch("travelcanary_pipeline.orchestration.dbt_build.Queue", EmptyOnceQueue),
        pytest.raises(RuntimeError, match="exit code 2"),
    ):
        list(
            stream_dbt_build(
                asset_name="risk",
                context=Mock(),
                dbt=dbt,
                config=DbtBuildConfig(progress_poll_seconds=1),
            )
        )


def test_stream_dbt_build_raises_producer_error(monkeypatch):

    class BrokenInvocation:
        process = Mock(returncode=0)

        def stream(self):
            raise RuntimeError("stream broke")
            yield

    dbt = Mock()
    dbt.cli.return_value = BrokenInvocation()

    with pytest.raises(RuntimeError, match="stream broke"):
        list(
            stream_dbt_build(
                asset_name="risk",
                context=Mock(),
                dbt=dbt,
                config=DbtBuildConfig(progress_poll_seconds=1),
            )
        )


def test_dbt_build_config_rejects_invalid_timeouts():
    with pytest.raises(ValueError, match="hard_timeout"):
        DbtBuildConfig(
            no_progress_soft_timeout_seconds=10,
            no_progress_hard_timeout_seconds=10,
        )


def test_stream_dbt_build_terminates_on_hard_timeout(monkeypatch):

    class EmptyQueue(Queue):
        def get(self, *args, **kwargs):
            raise __import__("queue").Empty

    class RecordingThread(Thread):
        join_timeouts: list[float | None] = []

        def join(self, timeout=None):
            self.join_timeouts.append(timeout)
            return super().join(timeout)

    invocation = _Invocation()
    invocation.process.poll.return_value = None
    invocation.process.wait.return_value = 0
    invocation.adapter = MagicMock()
    dbt = Mock()
    dbt.cli.return_value = invocation
    context = MagicMock()
    with (
        patch("travelcanary_pipeline.orchestration.dbt_build.Queue", EmptyQueue),
        patch("travelcanary_pipeline.orchestration.dbt_build.Thread", RecordingThread),
        patch(
            "travelcanary_pipeline.orchestration.dbt_build.time.monotonic",
            side_effect=[0.0, 2.0],
        ),
        pytest.raises(RuntimeError, match="hard timeout"),
    ):
        list(
            stream_dbt_build(
                asset_name="risk",
                context=context,
                dbt=dbt,
                config=DbtBuildConfig(
                    progress_poll_seconds=1,
                    no_progress_soft_timeout_seconds=1,
                    no_progress_hard_timeout_seconds=2,
                ),
            )
        )
    invocation.process.terminate.assert_called_once()
    assert RecordingThread.join_timeouts == [25.0]
    invocation.adapter.cleanup_connections.assert_called_once()
    invocation.adapter.connections.cleanup_all.assert_called_once()


def test_stop_dbt_process_returns_when_process_already_exited():
    invocation = Mock(termination_timeout_seconds=7)
    invocation.process.poll.return_value = 0

    assert _stop_dbt_process(invocation, asset_name="risk", context=MagicMock()) == 7.0
    invocation.process.terminate.assert_not_called()
    invocation.process.wait.assert_not_called()


def test_stop_dbt_process_waits_for_graceful_termination():
    invocation = Mock(termination_timeout_seconds=7)
    invocation.process.poll.return_value = None
    invocation.process.wait.return_value = 0

    assert _stop_dbt_process(invocation, asset_name="risk", context=MagicMock()) == 7.0
    invocation.process.terminate.assert_called_once()
    invocation.process.wait.assert_called_once_with(timeout=7.0)
    invocation.process.kill.assert_not_called()


def test_stop_dbt_process_reaps_process_that_exits_before_terminate():
    invocation = Mock(termination_timeout_seconds=7)
    invocation.process.poll.return_value = None
    invocation.process.terminate.side_effect = ProcessLookupError

    assert _stop_dbt_process(invocation, asset_name="risk", context=MagicMock()) == 7.0
    invocation.process.wait.assert_called_once_with(timeout=7.0)
    invocation.process.kill.assert_not_called()


def test_stop_dbt_process_escalates_to_kill_after_grace_period():
    invocation = Mock(termination_timeout_seconds=7)
    invocation.process.poll.return_value = None
    invocation.process.wait.side_effect = [
        subprocess.TimeoutExpired("dbt", 7),
        0,
    ]
    context = MagicMock()

    assert _stop_dbt_process(invocation, asset_name="risk", context=context) == 7.0
    invocation.process.terminate.assert_called_once()
    invocation.process.kill.assert_called_once()
    assert invocation.process.wait.call_args_list == [
        call(timeout=7.0),
        call(timeout=7.0),
    ]
    context.log.warning.assert_called_once()


def test_stream_dbt_build_warns_logs_and_resets_progress(monkeypatch):
    class EmptyAroundEventQueue(Queue):
        calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls in (1, 3):
                raise __import__("queue").Empty
            return super().get(*args, **kwargs)

    dbt = Mock()
    dbt.cli.return_value = _Invocation(events=["event"])
    context = MagicMock()
    with (
        patch(
            "travelcanary_pipeline.orchestration.dbt_build.Queue",
            EmptyAroundEventQueue,
        ),
        patch(
            "travelcanary_pipeline.orchestration.dbt_build.time.monotonic",
            side_effect=[0.0, 1.2, 1.3, 2.5],
        ),
    ):
        events = list(
            stream_dbt_build(
                asset_name="risk",
                context=context,
                dbt=dbt,
                config=DbtBuildConfig(
                    progress_poll_seconds=1,
                    progress_log_interval_seconds=1,
                    progress_log_interval_events=1,
                    no_progress_soft_timeout_seconds=1,
                    no_progress_hard_timeout_seconds=10,
                ),
            )
        )
    assert events == ["event"]
    assert context.log.warning.call_count == 2
    assert context.log.info.call_count == 3


def test_cleanup_dbt_adapter_calls_available_cleanup_methods():
    invocation = MagicMock()
    _cleanup_dbt_adapter(invocation)
    invocation.adapter.cleanup_connections.assert_called_once()
    invocation.adapter.connections.cleanup_all.assert_called_once()


def test_travelcanary_dbt_asset_passes_run_config(monkeypatch):
    from travelcanary_pipeline.orchestration import assets

    captured = {}
    monkeypatch.setattr(
        assets,
        "stream_dbt_build",
        lambda **kwargs: captured.update(kwargs) or iter(["event"]),
    )
    config = DbtBuildConfig(full_refresh=True)
    events = list(
        assets.travelcanary_dbt.op.compute_fn.decorated_fn(
            MagicMock(), MagicMock(), config
        )
    )
    assert events == ["event"]
    assert captured["config"] is config
