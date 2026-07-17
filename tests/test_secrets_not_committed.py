from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = (".env", "secrets.json", "credentials.json")


@pytest.mark.repo_check
def test_secrets_not_committed():
    for name in FORBIDDEN:
        assert not (REPO_ROOT / name).exists(), f"remove committed secret file: {name}"
