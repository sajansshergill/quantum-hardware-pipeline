from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_core_dbt_models_are_populated():
    model_paths = [
        ROOT / "dbt/models/bronze/raw_telemetry.sql",
        ROOT / "dbt/models/silver/stg_telemetry.sql",
        ROOT / "dbt/models/gold/fct_telemetry.sql",
        ROOT / "dbt/models/gold/device_reliability_mart.sql",
    ]

    for path in model_paths:
        sql = path.read_text().strip()
        assert "{{ ref(" in sql or "read_parquet" in sql
        assert len(sql.splitlines()) > 5


def test_dbt_profile_uses_env_driven_duckdb_path():
    profile = (ROOT / "dbt/profiles.yml").read_text()

    assert "DUCKDB_PATH" in profile
    assert "duckdb" in profile
