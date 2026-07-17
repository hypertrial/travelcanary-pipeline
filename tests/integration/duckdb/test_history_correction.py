from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from travelcanary_pipeline.ingestion.common.dlt_runner import run_advisory_load
from travelcanary_pipeline.naming import SOURCE_US_STATE, raw_schema
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection

REPO_ROOT = Path(__file__).resolve().parents[3]


def _dbt_build() -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt/profiles",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


@pytest.mark.integration
def test_same_utc_day_rerun_corrects_history_in_place():
    subprocess.run(
        [sys.executable, "scripts/seed_ci_warehouse.py"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    _dbt_build()

    conn = get_persistent_connection()
    try:
        conn.execute(
            """
            update us_state_raw.advisories
            set native_level = '4', native_level_label = 'Do not travel'
            where destination_iso2 = 'TH'
            """
        )
        conn.commit()
    finally:
        conn.close()

    _dbt_build()
    conn = get_persistent_connection()
    try:
        rows = conn.execute(
            """
            select normalized_ordinal
            from travelcanary_marts.country_travel_risk_history
            where destination_iso3 = 'THA' and issuing_government = 'us_state'
            """
        ).fetchall()
        assert rows == [(4,)]
    finally:
        conn.close()


@pytest.mark.integration
def test_two_day_history_drives_trend_and_change_marts():
    subprocess.run(
        [sys.executable, "scripts/seed_ci_warehouse.py"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    _dbt_build()

    conn = get_persistent_connection()
    try:
        conn.execute(
            """
            update travelcanary_marts.country_travel_risk_history
            set snapshot_date = current_date - interval 1 day
            """
        )
        conn.execute(
            """
            update us_state_raw.advisories
            set native_level = '4', native_level_label = 'Do not travel'
            where destination_iso2 = 'TH'
            """
        )
        conn.execute(
            """
            update ca_gac_raw.advisories
            set native_level = '4', native_level_label = 'Avoid all travel'
            where destination_iso2 = 'TH'
            """
        )
        conn.execute(
            """
            update uk_fcdo_raw.advisories
            set
                native_level = 'exercise_increased_caution_abroad',
                native_level_label = 'Exercise increased caution abroad'
            where destination_name = 'Thailand'
            """
        )
        conn.execute(
            """
            update nl_mfa_raw.advisories
            set native_level = 'rood', native_level_label = 'Red'
            where destination_iso3 = 'THA'
            """
        )
        conn.execute(
            """
            update jp_mofa_raw.advisories
            set native_level = '4', native_level_label = 'Level 4'
            where destination_name = 'Thailand'
            """
        )
        conn.commit()
    finally:
        conn.close()

    _dbt_build()
    conn = get_persistent_connection()
    try:
        trend = conn.execute(
            """
            select
                previous_normalized_ordinal_median,
                normalized_ordinal_median,
                risk_direction,
                previous_normalized_ordinal_range,
                normalized_ordinal_range,
                disagreement_direction
            from travelcanary_marts.country_risk_trends
            where destination_iso3 = 'THA'
              and snapshot_date = current_date
            """
        ).fetchone()
        assert trend == (2.0, 4.0, "worsening", 3, 2, "decreasing")

        changes = dict(
            conn.execute(
                """
                select issuing_government, advisory_direction
                from travelcanary_marts.country_advisory_changes
                where destination_iso3 = 'THA'
                  and snapshot_date = current_date
                  and issuing_government in ('us_state', 'uk_fcdo')
                """
            ).fetchall()
        )
        assert changes == {"us_state": "worsening", "uk_fcdo": "improving"}

        overview = conn.execute(
            """
            select risk_direction, disagreement_direction,
                   changed_issuer_count, worsening_issuer_count,
                   improving_issuer_count, context_alert_count
            from travelcanary_marts.country_risk_overview
            where destination_iso3 = 'THA'
            """
        ).fetchone()
        assert overview == ("worsening", "decreasing", 5, 4, 1, 0)
    finally:
        conn.close()


@pytest.mark.integration
def test_unaccepted_raw_append_cannot_displace_last_known_good_public_data():
    subprocess.run(
        [sys.executable, "scripts/seed_ci_warehouse.py"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    _dbt_build()

    conn = get_persistent_connection()
    try:
        expected = conn.execute(
            """
            select normalized_ordinal
            from travelcanary_marts.country_travel_risk
            where destination_iso3 = 'THA'
              and issuing_government = 'us_state'
            """
        ).fetchone()
        columns = [
            "source_run_id",
            "advisory_id",
            "source",
            "destination_native_id",
            "destination_iso2",
            "destination_iso3",
            "destination_name",
            "native_level",
            "native_level_label",
            "summary_text",
            "source_url",
            "published_at",
            "ingested_at",
        ]
        raw = conn.execute(
            f"""
            select {", ".join(columns)}
            from {raw_schema(SOURCE_US_STATE)}.advisories
            where destination_iso2 = 'TH'
            """
        ).fetchone()
    finally:
        conn.close()

    orphan = dict(zip(columns, raw, strict=True))
    orphan.update(
        source_run_id="unaccepted-append",
        native_level="4",
        native_level_label="Do not travel",
    )
    run_advisory_load(
        schema=raw_schema(SOURCE_US_STATE),
        source_name=SOURCE_US_STATE,
        rows=[orphan],
    )

    _dbt_build()
    conn = get_persistent_connection()
    try:
        actual = conn.execute(
            """
            select normalized_ordinal
            from travelcanary_marts.country_travel_risk
            where destination_iso3 = 'THA'
              and issuing_government = 'us_state'
            """
        ).fetchone()
        assert actual == expected
        assert conn.execute(
            """
            select count(*)
            from us_state_raw.advisories
            where destination_iso2 = 'TH'
            """
        ).fetchone() == (2,)
    finally:
        conn.close()
