from __future__ import annotations

from scripts import run_live_smoke

from travelcanary_pipeline import live_audit as audit
from travelcanary_pipeline.naming import SOURCE_US_STATE


def test_live_smoke_stops_on_required_audit_failure(monkeypatch, capsys):
    calls: list[list[str] | None] = []

    def _audit_sources(_target_date, *, selected_sources=None, propose_floors=False):
        calls.append(list(selected_sources) if selected_sources else None)
        return [
            {
                "source": SOURCE_US_STATE,
                "role": "required",
                "status": "error",
                "reason": "upstream timeout/no bytes",
                "rows": 0,
            }
        ]

    def _unexpected_materialization():
        raise AssertionError("live smoke should stop before materialization")

    monkeypatch.setattr(run_live_smoke, "_configure_disposable_warehouse", lambda: None)
    monkeypatch.setattr(run_live_smoke, "_reset_disposable_warehouse", lambda: None)
    monkeypatch.setattr(
        run_live_smoke, "_materialize_full_pipeline", _unexpected_materialization
    )
    monkeypatch.setattr(audit, "audit_sources", _audit_sources)

    assert run_live_smoke.main() == 1

    assert calls == [None]
    stderr = capsys.readouterr().err
    assert "full read-only source audit" in stderr
    assert "upstream timeout/no bytes" in stderr
