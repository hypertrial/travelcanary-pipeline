from pathlib import Path


def test_dbt_layers_exist():
    root = Path(__file__).resolve().parents[2] / "dbt" / "models"
    for layer in ("staging", "intermediate", "marts", "observability"):
        assert (root / layer).is_dir()
