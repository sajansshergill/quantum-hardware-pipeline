"""Build the local DuckDB demo dataset used by Streamlit Cloud."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build_demo_data(run_dbt_tests: bool = False) -> None:
    """Generate sample bronze data, run dbt, and write drift alerts."""
    sys.path.insert(0, str(ROOT))

    from detection.drift_detector import run_drift_detection
    from scripts.generate_bronze_sample import main as generate_bronze_sample

    generate_bronze_sample()
    subprocess.run(
        ["dbt", "run", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        cwd=ROOT,
        check=True,
    )
    if run_dbt_tests:
        subprocess.run(
            ["dbt", "test", "--project-dir", "dbt", "--profiles-dir", "dbt"],
            cwd=ROOT,
            check=True,
        )
    run_drift_detection()


def main() -> None:
    build_demo_data(run_dbt_tests=True)


if __name__ == "__main__":
    main()
