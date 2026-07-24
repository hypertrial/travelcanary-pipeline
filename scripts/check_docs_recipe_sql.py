"""Execute fenced SQL recipes from docs against the demo warehouse."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_WAREHOUSE = REPO_ROOT / ".cache" / "travelcanary_demo.duckdb"
RECIPES_DOC = REPO_ROOT / "docs" / "guides" / "query-recipes.md"
SQL_FENCE_RE = re.compile(r"```sql\s*\n(.*?)```", re.DOTALL | re.IGNORECASE)
COPY_TO_RE = re.compile(r"(?i)(\bTO\s+)(['\"])([^'\"]+)\2")


def _ensure_demo_warehouse() -> None:
    if DEMO_WAREHOUSE.is_file():
        return
    result = subprocess.run(["make", "demo"], cwd=REPO_ROOT, check=False)
    if result.returncode != 0 or not DEMO_WAREHOUSE.is_file():
        raise SystemExit("failed to create demo warehouse via make demo")


def _extract_sql_blocks(text: str) -> list[str]:
    return [block.strip() for block in SQL_FENCE_RE.findall(text) if block.strip()]


def _rewrite_copy_paths(sql: str) -> str:
    if "COPY" not in sql:
        return sql
    temp_dir = Path(tempfile.gettempdir())

    def _replace(match: re.Match[str]) -> str:
        rewritten = temp_dir / Path(match.group(3)).name
        return f"{match.group(1)}{match.group(2)}{rewritten}{match.group(2)}"

    return COPY_TO_RE.sub(_replace, sql)


def main() -> int:
    _ensure_demo_warehouse()
    blocks = _extract_sql_blocks(RECIPES_DOC.read_text(encoding="utf-8"))
    if not blocks:
        print("No SQL recipes found", file=sys.stderr)
        return 1

    conn = duckdb.connect(str(DEMO_WAREHOUSE), read_only=True)
    try:
        for index, block in enumerate(blocks, start=1):
            sql = _rewrite_copy_paths(block)
            try:
                conn.execute(sql)
            except duckdb.Error as exc:
                print(f"Recipe {index} failed:\n{sql}\n{exc}", file=sys.stderr)
                return 1
    finally:
        conn.close()

    print(f"Checked {len(blocks)} SQL recipes against {DEMO_WAREHOUSE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
