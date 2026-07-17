from __future__ import annotations

import importlib
import io
from pathlib import Path
from unittest.mock import Mock, PropertyMock, patch

import duckdb
import pytest

from travelcanary_pipeline.config import settings_warehouse
from travelcanary_pipeline.config._env import (
    _env_bool,
    _env_int,
    _optional_env_str,
)
from travelcanary_pipeline.resources.http import download_to_file, get_json, get_text
from travelcanary_pipeline.resources.outbound_url import (
    validate_outbound_http_url,
    validate_outbound_https_url,
)
from travelcanary_pipeline.storage.duckdb import connection


def test_environment_parsers_cover_defaults_valid_and_invalid(monkeypatch):
    monkeypatch.delenv("VALUE", raising=False)
    assert _env_int("VALUE", 3) == 3
    assert _env_bool("VALUE", True) is True
    assert _optional_env_str("VALUE") is None


@pytest.mark.parametrize("token", ["1", "true", "yes", "on", " TRUE "])
def test_environment_boolean_true_tokens(monkeypatch, token):
    monkeypatch.setenv("VALUE", token)
    assert _env_bool("VALUE", False) is True


@pytest.mark.parametrize("token", ["0", "false", "no", "off", " OFF "])
def test_environment_boolean_false_tokens(monkeypatch, token):
    monkeypatch.setenv("VALUE", token)
    assert _env_bool("VALUE", True) is False


@pytest.mark.parametrize("token", ["", " ", "maybe", "2"])
def test_environment_boolean_invalid_tokens(monkeypatch, token):
    monkeypatch.setenv("VALUE", token)
    with pytest.raises(ValueError, match="must be one of"):
        _env_bool("VALUE", True)


@pytest.mark.parametrize("token", ["", " ", "zero", "0", "-1"])
def test_environment_positive_integer_rejects_invalid_values(monkeypatch, token):
    monkeypatch.setenv("VALUE", token)
    with pytest.raises(ValueError, match="positive integer"):
        _env_int("VALUE", 3)

    monkeypatch.setenv("VALUE", " 42 ")
    assert _env_int("VALUE", 3) == 42
    assert _optional_env_str("VALUE") == "42"
    monkeypatch.setenv("VALUE", "yes")
    assert _env_bool("VALUE", False) is True
    monkeypatch.setenv("VALUE", "bad")
    with pytest.raises(ValueError, match="positive integer"):
        _env_int("VALUE", 3)
    with pytest.raises(ValueError, match="must be one of"):
        _env_bool("VALUE", True)
    monkeypatch.setenv("VALUE", "   ")
    assert _optional_env_str("VALUE") is None


def test_http_helpers_validate_and_decode_responses():
    response = Mock()
    response.json.return_value = {"ok": True}
    response.text = "body"
    response.iter_content.return_value = [b"by", b"", b"tes"]
    session = Mock()
    session.get.return_value = response
    with patch(
        "travelcanary_pipeline.resources.http._http_session", return_value=session
    ):
        assert get_json("https://example.com") == {"ok": True}
        assert get_text("https://example.com") == "body"
        destination = io.BytesIO()
        download_to_file("http://example.com", destination)
        assert destination.getvalue() == b"bytes"
    assert response.raise_for_status.call_count == 3
    response.close.assert_called_once()


def test_download_to_file_never_reads_buffered_response_content():
    response = Mock()
    response.iter_content.return_value = [b"chunk"]
    type(response).content = PropertyMock(
        side_effect=AssertionError("response.content must not be read")
    )
    session = Mock()
    session.get.return_value = response

    with patch(
        "travelcanary_pipeline.resources.http._http_session", return_value=session
    ):
        destination = io.BytesIO()
        download_to_file("https://example.com/export.zip", destination)

    assert destination.getvalue() == b"chunk"
    response.iter_content.assert_called_once_with(chunk_size=1024 * 1024)
    response.close.assert_called_once()


def test_http_session_mounts_retry_adapters_and_reuses_session(monkeypatch):
    from travelcanary_pipeline.resources import http

    session = Mock()
    monkeypatch.setattr(http, "_SESSION", None)
    monkeypatch.setattr(http.requests, "Session", Mock(return_value=session))

    first = http._http_session()
    second = http._http_session()

    assert first is second is session
    assert session.mount.call_count == 2
    assert [call.args[0] for call in session.mount.call_args_list] == [
        "https://",
        "http://",
    ]


def test_outbound_urls_require_a_host_and_supported_scheme():
    with pytest.raises(ValueError, match="invalid URL"):
        validate_outbound_https_url("https:///missing")
    with pytest.raises(ValueError, match="only http"):
        validate_outbound_http_url("file:///tmp/data")
    with pytest.raises(ValueError, match="invalid URL"):
        validate_outbound_http_url("http:///missing")


def test_duckdb_path_switch_and_lock_fallback(tmp_path, monkeypatch):
    relative = "relative-test.duckdb"
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", relative)
    connection.reset_duckdb_connection_state()
    assert connection.active_duckdb_path().name == relative

    absolute = tmp_path / "absolute.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(absolute))
    assert connection._sync_active_duckdb_path() == absolute
    assert not connection.is_duckdb_lock_io_error(RuntimeError("lock"))
    assert connection.is_duckdb_lock_io_error(
        duckdb.IOException("Conflicting lock on file")
    )

    fallback = duckdb.connect(":memory:")
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "lock fallback")
    with patch(
        "travelcanary_pipeline.storage.duckdb.connection.duckdb.connect",
        side_effect=[duckdb.IOException("Conflicting lock"), fallback],
    ):
        assert connection._connect_duckdb(tmp_path / "locked.duckdb") is fallback
    fallback.close()


def test_duckdb_default_path_and_non_lock_io_error(monkeypatch):
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.delenv("DUCKDB_NAME", raising=False)
    connection.reset_duckdb_connection_state()
    assert connection._resolved_duckdb_path() == connection._settings.DUCKDB_PATH

    with (
        patch(
            "travelcanary_pipeline.storage.duckdb.connection.duckdb.connect",
            side_effect=duckdb.IOException("disk full"),
        ),
        pytest.raises(duckdb.IOException, match="disk full"),
    ):
        connection._connect_duckdb()


def test_duckdb_blank_values_are_unset_and_directories_fail(tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_PATH", "   ")
    monkeypatch.setenv("DUCKDB_NAME", "   ")
    reloaded = importlib.reload(settings_warehouse)
    assert reloaded.DUCKDB_NAME == "travelcanary.duckdb"
    assert (
        reloaded.DUCKDB_PATH
        == (settings_warehouse.BASE_DIR / "travelcanary.duckdb").resolve()
    )

    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path))
    with pytest.raises(ValueError, match="must be a file, not a directory"):
        importlib.reload(settings_warehouse)


def test_settings_warehouse_env_profiles_and_dbt_resolution(tmp_path, monkeypatch):
    try:
        profiles = tmp_path / "profiles"
        profiles.mkdir()
        (profiles / "profiles.yml").write_text(
            """
travelcanary:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: warehouse.duckdb
"""
        )
        monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles))

        with patch(
            "travelcanary_pipeline.config.settings_warehouse.load_dotenv"
        ) as load_dotenv:
            reloaded = importlib.reload(settings_warehouse)

        assert reloaded.DBT_PROFILES_DIR == profiles
        load_dotenv.assert_not_called()

        monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path / "missing"))
        with pytest.raises(ValueError, match="DBT_PROFILES_DIR"):
            importlib.reload(settings_warehouse)

        incomplete_profiles = tmp_path / "incomplete-profiles"
        incomplete_profiles.mkdir()
        monkeypatch.setenv("DBT_PROFILES_DIR", str(incomplete_profiles))
        for invalid_profile in (
            "[]\n",
            "travelcanary: null\n",
            "travelcanary:\n  target: 1\n  outputs: {}\n",
            "travelcanary:\n  target: '  '\n  outputs: {}\n",
            "travelcanary:\n  target: dev\n  outputs: []\n",
            "travelcanary:\n  target: dev\n  outputs:\n    other: {}\n",
            "travelcanary:\n  target: dev\n  outputs:\n    dev: null\n",
            "travelcanary:\n  target: dev\n  outputs:\n    dev: {}\n",
            "travelcanary:\n  target: dev\n  outputs:\n    dev:\n      type: postgres\n      path: warehouse.duckdb\n",
            "travelcanary:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n",
            "travelcanary:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: '  '\n",
        ):
            (incomplete_profiles / "profiles.yml").write_text(invalid_profile)
            with pytest.raises(ValueError, match="valid travelcanary profile"):
                importlib.reload(settings_warehouse)

        invalid_profiles = tmp_path / "invalid-profiles"
        invalid_profiles.mkdir()
        (invalid_profiles / "profiles.yml").write_text("# travelcanary:\nother: {}\n")
        monkeypatch.setenv("DBT_PROFILES_DIR", str(invalid_profiles))
        with pytest.raises(ValueError, match="DBT_PROFILES_DIR"):
            importlib.reload(settings_warehouse)

        malformed_profiles = tmp_path / "malformed-profiles"
        malformed_profiles.mkdir()
        (malformed_profiles / "profiles.yml").write_text("travelcanary: [\n")
        monkeypatch.setenv("DBT_PROFILES_DIR", str(malformed_profiles))
        with pytest.raises(ValueError, match="DBT_PROFILES_DIR"):
            importlib.reload(settings_warehouse)

        monkeypatch.setenv("DBT_PROFILES_DIR", "   ")
        assert importlib.reload(settings_warehouse).DBT_PROFILES_DIR == (
            settings_warehouse.BASE_DIR / "dbt" / "profiles"
        )
        monkeypatch.delenv("DBT_PROFILES_DIR")

        monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "warehouse.duckdb"))
        with (
            patch.object(Path, "exists", lambda path: path.name == ".env"),
            patch("dotenv.load_dotenv") as load_dotenv,
        ):
            reloaded = importlib.reload(settings_warehouse)
        load_dotenv.assert_called_once()
        assert reloaded.DUCKDB_PATH == (tmp_path / "warehouse.duckdb").resolve()

        fake_dbt = tmp_path / "dbt"
        fake_dbt.write_text("")
        monkeypatch.setattr(
            settings_warehouse.sys, "executable", str(tmp_path / "python")
        )
        assert settings_warehouse.resolve_dbt_executable() == str(fake_dbt)

        fake_dbt.unlink()
        monkeypatch.setattr(settings_warehouse.shutil, "which", lambda name: None)
        assert settings_warehouse.resolve_dbt_executable() == "dbt"
    finally:
        monkeypatch.undo()
        importlib.reload(settings_warehouse)
