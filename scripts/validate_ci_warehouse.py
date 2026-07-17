"""Assert the public contract against the deterministic CI warehouse."""

from __future__ import annotations

from travelcanary_pipeline.ingestion.source_contracts import load_source_contracts
from travelcanary_pipeline.public_contracts import PUBLIC_MART_COLUMNS
from travelcanary_pipeline.storage.duckdb.connection import get_persistent_connection


def _columns(conn, relation: str) -> list[str]:
    return [row[0] for row in conn.execute(f"describe {relation}").fetchall()]


def main() -> None:
    required_source_count = len(load_source_contracts())
    conn = get_persistent_connection()
    try:
        relations = {mart: f"travelcanary_marts.{mart}" for mart in PUBLIC_MART_COLUMNS}
        current = relations["country_travel_risk"]
        signals = relations["country_risk_signals"]
        overview = relations["country_risk_overview"]
        trends = relations["country_risk_trends"]
        themes = relations["country_advisory_themes"]
        event_types = relations["country_gdelt_event_types"]
        alerts = relations["country_context_alerts"]
        quality = relations["source_data_quality"]
        for mart, relation in relations.items():
            assert _columns(conn, relation) == PUBLIC_MART_COLUMNS[mart]

        duplicate_count = conn.execute(
            f"""
            select count(*) from (
                select destination_iso3, issuing_government, snapshot_date
                from {current}
                group by 1, 2, 3 having count(*) > 1
            )
            """
        ).fetchone()[0]
        assert duplicate_count == 0

        thai = conn.execute(
            f"""
            select reporting_issuer_count, normalized_ordinal_min,
                   normalized_ordinal_median, normalized_ordinal_max,
                   normalized_ordinal_range, gdelt_event_count_1d,
                   gdelt_event_count_7d
            from {signals} where destination_iso3 = 'THA'
            """
        ).fetchone()
        assert thai == (5, 1, 2.0, 4, 3, 6, 7)

        thai_overview = conn.execute(
            f"""
            select reporting_issuers, matched_theme_count, matched_themes,
                   context_alert_count, context_alert_types,
                   has_warning_context_alert, required_source_count,
                   usable_required_source_count, all_required_sources_usable,
                   gdelt_source_usable
            from {overview} where destination_iso3 = 'THA'
            """
        ).fetchone()
        assert thai_overview == (
            "ca_gac, jp_mofa, nl_mfa, uk_fcdo, us_state",
            5,
            "conflict, crime, health, natural_disasters, terrorism",
            1,
            "official_low_gdelt_high",
            True,
            required_source_count,
            required_source_count,
            True,
            True,
        )

        swiss_overview = conn.execute(
            f"""
            select matched_theme_count, matched_themes, context_alert_count,
                   context_alert_types, has_warning_context_alert
            from {overview} where destination_iso3 = 'CHE'
            """
        ).fetchone()
        assert swiss_overview == (0, None, 0, None, False)

        thai_trend = conn.execute(
            f"""
            select risk_direction, disagreement_direction, has_high_disagreement
            from {trends} where destination_iso3 = 'THA'
            """
        ).fetchone()
        assert thai_trend == ("new", "new", True)

        thai_theme_count = conn.execute(
            f"""
            select count(distinct theme)
            from {themes}
            where destination_iso3 = 'THA'
              and issuing_government = 'us_state'
            """
        ).fetchone()[0]
        assert thai_theme_count == 5

        thai_fight_events = conn.execute(
            f"""
            select event_count, mention_count
            from {event_types}
            where destination_iso3 = 'THA'
              and event_root_code = '19'
            """
        ).fetchone()
        assert thai_fight_events == (5, 30)

        thai_alert = conn.execute(
            f"""
            select alert_type, severity, gdelt_mention_count_7d
            from {alerts}
            where destination_iso3 = 'THA'
            """
        ).fetchone()
        assert thai_alert == ("official_low_gdelt_high", "warning", 35)

        quality_sources = conn.execute(f"select count(*) from {quality}").fetchone()[0]
        assert quality_sources == required_source_count

        assert (
            conn.execute(
                f"select count(*) from {current} where destination_iso3 is null"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                f"select normalization_status from {current} where destination_iso3 = 'CHE'"
            ).fetchone()[0]
            == "unmapped"
        )
        assert (
            conn.execute(
                """
            select count(*) from travelcanary_observability.country_crosswalk_gaps
            where destination_native_id = 'PT-20'
            """
            ).fetchone()[0]
            == 1
        )
    finally:
        conn.close()
    print("Seeded warehouse public contracts validated.")


if __name__ == "__main__":
    main()
