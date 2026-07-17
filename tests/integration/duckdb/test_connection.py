import pytest

from travelcanary_pipeline.storage.duckdb.connection import (
    ensure_duck_db,
    get_persistent_connection,
)


@pytest.mark.integration
def test_duckdb_bootstraps_raw_schemas():
    ensure_duck_db()
    conn = get_persistent_connection()
    try:
        schemas = {
            row[0]
            for row in conn.execute(
                "select schema_name from information_schema.schemata"
            ).fetchall()
        }
        assert "us_state_raw" in schemas
        assert "gdelt_raw" in schemas
    finally:
        conn.close()
