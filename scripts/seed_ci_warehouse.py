"""Seed a disposable DuckDB warehouse for CI dbt builds."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from travelcanary_pipeline.ingestion.common.advisory import (
    row_from_parts,
    rows_to_dicts,
)
from travelcanary_pipeline.ingestion.common.dlt_runner import (
    run_advisory_load,
)
from travelcanary_pipeline.ingestion.gdelt.events import upsert_gdelt_events
from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.naming import (
    ALL_ADVISORY_SOURCES,
    SOURCE_GDELT,
    raw_schema,
)
from travelcanary_pipeline.storage.duckdb.connection import (
    active_duckdb_path,
    ensure_duck_db,
    get_persistent_connection,
    reset_duckdb_connection_state,
)
from travelcanary_pipeline.storage.duckdb.source_runs import record_source_run


def _rows_for_source(source: str, ingested: str):
    native_level = {
        "us_state": "1",
        "ca_gac": "2",
        "uk_fcdo": "avoid_all_travel_to_whole_country",
        "nl_mfa": "geel",
        "jp_mofa": "1",
    }[source]
    native_id = {
        "uk_fcdo": "thailand",
        "nl_mfa": "THA",
        "jp_mofa": "0066",
    }.get(source, "TH")
    rows = [
        row_from_parts(
            source=source,
            destination_native_id=native_id,
            destination_iso2="TH" if source in {"us_state", "ca_gac"} else None,
            destination_iso3="THA" if source == "nl_mfa" else None,
            destination_name="Thailand",
            native_level=native_level,
            native_level_label=native_level,
            summary_text=(
                "Terrorism and crime remain elevated after flooding and "
                "armed conflict disrupted health services."
                if source == "us_state"
                else "Offline CI fixture."
            ),
            source_url=f"https://example.com/{source}/thailand",
            ingested_at=ingested,
        )
    ]
    if source == "us_state":
        rows.append(
            row_from_parts(
                source=source,
                destination_native_id="CHE-only",
                destination_iso3="CHE",
                destination_name="Switzerland",
                native_level="unknown",
                native_level_label="Unknown provider-native level",
                source_url="https://example.com/us_state/switzerland",
                ingested_at=ingested,
            )
        )
    if source == "ca_gac":
        rows.append(
            row_from_parts(
                source=source,
                destination_native_id="PT-20",
                destination_name="Azores regional record",
                native_level="2",
                native_level_label="Exercise a high degree of caution",
                source_url="https://example.com/ca_gac/pt-20",
                ingested_at=ingested,
            )
        )
    if source == "jp_mofa":
        rows.append(
            row_from_parts(
                source=source,
                destination_native_id="0380",
                destination_name="Ukraine",
                native_level=None,
                native_level_label=None,
                source_url="https://example.com/jp_mofa/0380",
                ingested_at=ingested,
            )
        )
    return rows


def _record_accepted(
    *,
    source: str,
    source_run_id: str,
    row_count: int,
    contracts,
    started: datetime,
) -> None:
    conn = get_persistent_connection()
    try:
        record_source_run(
            conn,
            source_run_id=source_run_id,
            source=source,
            contract=contracts[source],
            status="accepted",
            started_at=started,
            fetched_rows=row_count,
            previous_rows=None,
            canonical_ratio=1.0,
            normalization_ratio=1.0,
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    contracts = load_source_contracts()
    from travelcanary_pipeline.storage.duckdb.writer_lock import (
        warehouse_writer_lock,
    )

    reset_duckdb_connection_state()
    warehouse = active_duckdb_path()
    with warehouse_writer_lock(warehouse):
        _reset_and_seed(warehouse, contracts)


def _reset_and_seed(warehouse, contracts) -> None:
    reset_duckdb_connection_state()
    for path in (
        warehouse,
        warehouse.with_name(f"{warehouse.name}.wal"),
        warehouse.with_name(f"{warehouse.name}-wal"),
        warehouse.with_name(f"{warehouse.name}-shm"),
    ):
        path.unlink(missing_ok=True)
    _seed(contracts)


def _seed(contracts) -> None:
    reset_duckdb_connection_state()
    ensure_duck_db()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    ingested = now.isoformat()

    for source in ALL_ADVISORY_SOURCES:
        source_run_id = str(uuid4())
        rows = rows_to_dicts(_rows_for_source(source, ingested))
        for row in rows:
            row["source_run_id"] = source_run_id
        run_advisory_load(
            schema=raw_schema(source),
            source_name=source,
            rows=rows,
        )
        _record_accepted(
            source=source,
            source_run_id=source_run_id,
            row_count=len(rows),
            contracts=contracts,
            started=now,
        )

    for index, days_ago in enumerate((1, 5), start=1):
        event_date = (now.date() - timedelta(days=days_ago)).strftime("%Y%m%d")
        gdelt_run_id = str(uuid4())
        if days_ago == 1:
            event_specs = [
                ("19", 4, -6.0, -3.0, 6),
                ("19", 4, -6.0, -3.0, 6),
                ("19", 4, -6.0, -3.0, 6),
                ("19", 4, -6.0, -3.0, 6),
                ("19", 4, -6.0, -3.0, 6),
                ("14", 3, -2.0, -1.0, 3),
            ]
        else:
            event_specs = [("18", 4, -4.0, -2.0, 2)]
        conn = get_persistent_connection()
        try:
            upsert_gdelt_events(
                conn,
                [
                    {
                        "source_run_id": gdelt_run_id,
                        "global_event_id": f"9000000{index}{offset}",
                        "event_date": event_date,
                        "action_geo_country_code": "TH",
                        "event_root_code": root_code,
                        "quad_class": quad_class,
                        "goldstein_scale": goldstein_scale,
                        "avg_tone": avg_tone,
                        "num_mentions": mentions,
                        "date_added": f"{event_date}120000",
                        "source_url": f"https://example.com/gdelt/{index}/{offset}",
                        "ingested_at": ingested,
                    }
                    for offset, (
                        root_code,
                        quad_class,
                        goldstein_scale,
                        avg_tone,
                        mentions,
                    ) in enumerate(event_specs, start=1)
                ],
            )
            conn.commit()
        finally:
            conn.close()
        _record_accepted(
            source=SOURCE_GDELT,
            source_run_id=gdelt_run_id,
            row_count=len(event_specs),
            contracts=contracts,
            started=now,
        )

    print("CI warehouse seeded with multi-source contract fixtures.")


if __name__ == "__main__":
    main()
