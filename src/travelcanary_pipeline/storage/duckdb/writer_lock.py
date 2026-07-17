"""Cross-process serialization for supported DuckDB writers."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


def writer_lock_path(warehouse_path: Path) -> Path:
    return Path(f"{warehouse_path}.writer.lock")


@contextmanager
def warehouse_writer_lock(warehouse_path: Path) -> Iterator[None]:
    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - supported hosts are POSIX
        raise RuntimeError(
            "TravelCanary warehouse writes require macOS or Linux file locking"
        ) from exc

    warehouse_path = warehouse_path.expanduser().resolve()
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = writer_lock_path(warehouse_path)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"warehouse writer already active for {warehouse_path}; "
                "wait for it to finish before retrying"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


__all__ = ["warehouse_writer_lock", "writer_lock_path"]
