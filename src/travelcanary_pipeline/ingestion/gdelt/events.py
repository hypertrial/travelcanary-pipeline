"""GDELT 1.0 Events complete daily-export ingestion."""

from __future__ import annotations

import csv
import io
import tempfile
import zipfile
from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta, timezone
from itertools import islice
from typing import BinaryIO, Callable
from uuid import uuid4

import duckdb

from travelcanary_pipeline.config.settings import GDELT_ROLLING_WINDOW_DAYS
from travelcanary_pipeline.ingestion.common.advisory import BatchDiagnostics
from travelcanary_pipeline.naming import SOURCE_GDELT, raw_schema
from travelcanary_pipeline.resources.http import download_to_file

GDELT_DAILY_EXPORT_URL = (
    "https://storage.googleapis.com/data.gdeltproject.org/"
    "events/{yyyymmdd}.export.CSV.zip"
)

_GDELT_EVENT_COLUMNS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]

_SELECTED_COLUMNS = [
    "source_run_id",
    "global_event_id",
    "event_date",
    "action_geo_country_code",
    "event_root_code",
    "quad_class",
    "goldstein_scale",
    "avg_tone",
    "num_mentions",
    "date_added",
    "source_url",
    "ingested_at",
]
_INSERT_BATCH_SIZE = 5_000


def _daily_export_url(for_date: date) -> str:
    return GDELT_DAILY_EXPORT_URL.format(yyyymmdd=for_date.strftime("%Y%m%d"))


def iter_gdelt_export_zip(
    zip_file: BinaryIO,
    *,
    ingested_at: str,
    diagnostics: BatchDiagnostics | None = None,
) -> Iterator[dict[str, object]]:
    diagnostics = diagnostics or BatchDiagnostics()
    with zipfile.ZipFile(zip_file) as archive:
        names = [name for name in archive.namelist() if name.endswith(".CSV")]
        if not names:
            raise ValueError("GDELT zip missing CSV export")
        with (
            archive.open(names[0]) as raw,
            io.TextIOWrapper(
                raw, encoding="utf-8", errors="replace", newline=""
            ) as text,
        ):
            reader = csv.reader(text, delimiter="\t")
            for values in reader:
                if len(values) != len(_GDELT_EVENT_COLUMNS):
                    diagnostics.skip("malformed export row")
                    continue
                diagnostics.observe()
                record = dict(zip(_GDELT_EVENT_COLUMNS, values, strict=False))
                yield {
                    "source_run_id": "",
                    "global_event_id": record["GLOBALEVENTID"],
                    "event_date": record["SQLDATE"],
                    "action_geo_country_code": (
                        record["ActionGeo_CountryCode"] or None
                    ),
                    "event_root_code": record["EventRootCode"] or None,
                    "quad_class": _safe_int(record.get("QuadClass")),
                    "goldstein_scale": _safe_float(record.get("GoldsteinScale")),
                    "avg_tone": _safe_float(record.get("AvgTone")),
                    "num_mentions": _safe_int(record.get("NumMentions")),
                    "date_added": record.get("DATEADDED"),
                    "source_url": record.get("SOURCEURL"),
                    "ingested_at": ingested_at,
                }


def _safe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _safe_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except ValueError:
        return None


def ensure_gdelt_events_table(conn: duckdb.DuckDBPyConnection) -> None:
    schema = raw_schema(SOURCE_GDELT)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.events (
            source_run_id VARCHAR NOT NULL,
            global_event_id VARCHAR PRIMARY KEY,
            event_date VARCHAR,
            action_geo_country_code VARCHAR,
            event_root_code VARCHAR,
            quad_class INTEGER,
            goldstein_scale DOUBLE,
            avg_tone DOUBLE,
            num_mentions INTEGER,
            date_added VARCHAR,
            source_url VARCHAR,
            ingested_at VARCHAR
        )
        """
    )


def upsert_gdelt_events(
    conn: duckdb.DuckDBPyConnection, rows: Iterable[dict[str, object]]
) -> int:
    values = (tuple(row[col] for col in _SELECTED_COLUMNS) for row in rows)
    batch = list(islice(values, _INSERT_BATCH_SIZE))
    if not batch:
        return 0
    ensure_gdelt_events_table(conn)
    schema = raw_schema(SOURCE_GDELT)
    sql = f"""
    INSERT OR REPLACE INTO {schema}.events
    ({", ".join(_SELECTED_COLUMNS)})
    VALUES ({", ".join("?" for _ in _SELECTED_COLUMNS)})
    """
    inserted = 0
    while batch:
        conn.executemany(sql, batch)
        inserted += len(batch)
        batch = list(islice(values, _INSERT_BATCH_SIZE))
    return inserted


def prune_gdelt_events(
    conn: duckdb.DuckDBPyConnection, *, window_days: int = GDELT_ROLLING_WINDOW_DAYS
) -> int:
    ensure_gdelt_events_table(conn)
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=window_days)).strftime(
        "%Y%m%d"
    )
    schema = raw_schema(SOURCE_GDELT)
    before = conn.execute(f"SELECT COUNT(*) FROM {schema}.events").fetchone()[0]
    conn.execute(
        f"DELETE FROM {schema}.events WHERE CAST(event_date AS VARCHAR) < ?",
        [cutoff],
    )
    after = conn.execute(f"SELECT COUNT(*) FROM {schema}.events").fetchone()[0]
    return int(before - after)


def sync_gdelt_daily_events(
    *,
    for_date: date | None = None,
    fetch_file: Callable[[str, BinaryIO], None] = download_to_file,
    conn: duckdb.DuckDBPyConnection | None = None,
    enforce_contract: bool = True,
) -> dict[str, object]:
    if conn is not None:
        return _sync_gdelt_daily_events_unlocked(
            for_date=for_date,
            fetch_file=fetch_file,
            conn=conn,
            enforce_contract=enforce_contract,
        )
    from travelcanary_pipeline.storage.duckdb.connection import active_duckdb_path
    from travelcanary_pipeline.storage.duckdb.writer_lock import (
        warehouse_writer_lock,
    )

    with warehouse_writer_lock(active_duckdb_path()):
        return _sync_gdelt_daily_events_unlocked(
            for_date=for_date,
            fetch_file=fetch_file,
            conn=None,
            enforce_contract=enforce_contract,
        )


def _sync_gdelt_daily_events_unlocked(
    *,
    for_date: date | None = None,
    fetch_file: Callable[[str, BinaryIO], None] = download_to_file,
    conn: duckdb.DuckDBPyConnection | None = None,
    enforce_contract: bool = True,
) -> dict[str, object]:
    from travelcanary_pipeline.storage.duckdb.connection import (
        ensure_duck_db,
        get_persistent_connection,
    )

    target_date = for_date or (datetime.now(timezone.utc).date() - timedelta(days=1))
    started_at = datetime.now(timezone.utc)
    ingested_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    source_run_id = str(uuid4())
    diagnostics = BatchDiagnostics()
    url = _daily_export_url(target_date)
    contract = None
    previous_rows = None
    mapped_codes: set[str] = set()
    if enforce_contract:
        from travelcanary_pipeline.ingestion.source_contracts import (
            load_source_contracts,
        )
        from travelcanary_pipeline.storage.duckdb.source_runs import (
            evaluate_source_run_metrics,
            previous_accepted_rows,
            record_source_run_status,
        )

        contract = load_source_contracts()[SOURCE_GDELT]
    owns_conn = conn is None
    if conn is None:
        ensure_duck_db()
        conn = get_persistent_connection()
    try:
        if enforce_contract:
            previous_rows = previous_accepted_rows(conn, SOURCE_GDELT)
            mapped_codes = _load_fips_codes()

        with tempfile.TemporaryFile() as zip_file:
            try:
                fetch_file(url, zip_file)
            except Exception as exc:
                if enforce_contract:
                    record_source_run_status(
                        conn,
                        source_run_id=source_run_id,
                        source=SOURCE_GDELT,
                        contract=contract,
                        status="error",
                        started_at=started_at,
                        fetched_rows=0,
                        previous_rows=previous_rows,
                        canonical_ratio=None,
                        normalization_ratio=None,
                        message=str(exc),
                    )
                    conn.commit()
                raise
            zip_file.seek(0)
            fetched_rows = 0
            geographic_rows = 0
            mapped_rows = 0

            def observed_rows() -> Iterator[dict[str, object]]:
                nonlocal fetched_rows, geographic_rows, mapped_rows
                for row in iter_gdelt_export_zip(
                    zip_file,
                    ingested_at=ingested_at,
                    diagnostics=diagnostics,
                ):
                    row["source_run_id"] = source_run_id
                    fetched_rows += 1
                    country_code = row["action_geo_country_code"]
                    if country_code:
                        geographic_rows += 1
                        if country_code in mapped_codes:
                            mapped_rows += 1
                    yield row

            canonical_ratio = 0.0
            rejection_reason = None
            conn.execute("BEGIN TRANSACTION")
            try:
                inserted = upsert_gdelt_events(conn, observed_rows())
                canonical_ratio = (
                    mapped_rows / geographic_rows if geographic_rows else 0.0
                )
                if diagnostics.blocking_skipped_rows:
                    rejection_reason = (
                        f"malformed source records: {diagnostics.message()}"
                    )
                elif enforce_contract:
                    evaluation = evaluate_source_run_metrics(
                        contract=contract,
                        fetched_rows=fetched_rows,
                        previous_rows=previous_rows,
                        canonical_ratio=canonical_ratio,
                        normalization_ratio=1.0,
                    )
                    rejection_reason = evaluation.rejection_reason
                if rejection_reason:
                    conn.rollback()
                else:
                    pruned = prune_gdelt_events(conn)
                    if enforce_contract:
                        record_source_run_status(
                            conn,
                            source_run_id=source_run_id,
                            source=SOURCE_GDELT,
                            contract=contract,
                            status="accepted",
                            started_at=started_at,
                            fetched_rows=fetched_rows,
                            previous_rows=previous_rows,
                            canonical_ratio=canonical_ratio,
                            normalization_ratio=1.0,
                            message=diagnostics.message(),
                        )
                    conn.commit()
            except Exception as exc:
                conn.rollback()
                if enforce_contract:
                    record_source_run_status(
                        conn,
                        source_run_id=source_run_id,
                        source=SOURCE_GDELT,
                        contract=contract,
                        status="error",
                        started_at=started_at,
                        fetched_rows=fetched_rows,
                        previous_rows=previous_rows,
                        canonical_ratio=canonical_ratio,
                        normalization_ratio=1.0,
                        message=str(exc),
                    )
                    conn.commit()
                raise
            if rejection_reason:
                if enforce_contract:
                    record_source_run_status(
                        conn,
                        source_run_id=source_run_id,
                        source=SOURCE_GDELT,
                        contract=contract,
                        status="rejected",
                        started_at=started_at,
                        fetched_rows=fetched_rows,
                        previous_rows=previous_rows,
                        canonical_ratio=canonical_ratio,
                        normalization_ratio=1.0,
                        message=rejection_reason,
                    )
                    conn.commit()
                raise ValueError(f"gdelt batch rejected: {rejection_reason}")
    finally:
        if owns_conn:
            conn.close()
    return {
        "source": SOURCE_GDELT,
        "source_run_id": source_run_id,
        "url": url,
        "event_date": target_date.isoformat(),
        "rows": inserted,
        "pruned_rows": pruned,
        **diagnostics.summary(),
    }


def _load_fips_codes() -> set[str]:
    import csv

    from travelcanary_pipeline.config.settings import BASE_DIR

    with (BASE_DIR / "dbt" / "seeds" / "fips10_4_to_iso3.csv").open(
        encoding="utf-8"
    ) as handle:
        return {row["fips10_4"] for row in csv.DictReader(handle)}


__all__ = [
    "GDELT_DAILY_EXPORT_URL",
    "ensure_gdelt_events_table",
    "iter_gdelt_export_zip",
    "prune_gdelt_events",
    "sync_gdelt_daily_events",
    "upsert_gdelt_events",
]
