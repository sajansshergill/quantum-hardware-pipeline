"""Rolling z-score drift detection for QPU telemetry."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, List

import duckdb
import pandas as pd


@dataclass
class DriftAlert:
    device_id: str
    qubit_id: int
    timestamp: str
    metric: str
    value: float
    rolling_mean: float
    rolling_stddev: float
    zscore: float


def compute_drift_alerts(
    telemetry: pd.DataFrame,
    window: int = 30,
    threshold: float = 2.5,
    min_history: int = 5,
) -> List[DriftAlert]:
    """Return alerts where gate-error metrics breach a rolling z-score threshold."""
    if telemetry.empty:
        return []

    frame = telemetry.sort_values(["device_id", "qubit_id", "timestamp"]).copy()
    alerts: List[DriftAlert] = []
    for metric in ("gate_error_1q", "gate_error_2q"):
        grouped = frame.groupby(["device_id", "qubit_id"], group_keys=False)[metric]
        frame[f"{metric}_mean"] = grouped.transform(
            lambda values: values.shift(1).rolling(window, min_periods=min_history).mean()
        )
        frame[f"{metric}_std"] = grouped.transform(
            lambda values: values.shift(1).rolling(window, min_periods=min_history).std()
        )
        frame[f"{metric}_zscore"] = (
            (frame[metric] - frame[f"{metric}_mean"]) / frame[f"{metric}_std"].replace(0, pd.NA)
        )

        breached = frame[frame[f"{metric}_zscore"].abs() >= threshold]
        for row in breached.itertuples(index=False):
            alerts.append(
                DriftAlert(
                    device_id=row.device_id,
                    qubit_id=int(row.qubit_id),
                    timestamp=str(row.timestamp),
                    metric=metric,
                    value=float(getattr(row, metric)),
                    rolling_mean=float(getattr(row, f"{metric}_mean")),
                    rolling_stddev=float(getattr(row, f"{metric}_std")),
                    zscore=float(getattr(row, f"{metric}_zscore")),
                )
            )
    return alerts


def run_drift_detection(
    duckdb_path: str | Path | None = None,
    alerts_path: str | Path | None = None,
    threshold: float = 2.5,
) -> List[DriftAlert]:
    db_path = Path(duckdb_path or os.getenv("DUCKDB_PATH", "data/lakehouse/qpu_pipeline.duckdb"))
    output_path = Path(alerts_path or os.getenv("DRIFT_ALERTS_PATH", "data/alerts/drift_alerts.json"))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as conn:
        relation = _find_relation(conn, "fct_telemetry")
        telemetry = conn.execute(
            f"""
            select device_id, qubit_id, timestamp, gate_error_1q, gate_error_2q
            from {relation}
            order by device_id, qubit_id, timestamp
            """
        ).df()

    alerts = compute_drift_alerts(telemetry, threshold=threshold)
    output_path.write_text(json.dumps([asdict(alert) for alert in alerts], indent=2))
    return alerts


def _find_relation(conn: duckdb.DuckDBPyConnection, table_name: str) -> str:
    rows = conn.execute(
        """
        select table_schema, table_name
        from information_schema.tables
        where table_name = ?
        order by table_schema
        """,
        [table_name],
    ).fetchall()
    if not rows:
        raise RuntimeError(f"Could not find table {table_name!r}; run dbt first.")
    schema, table = rows[0]
    return f'"{schema}"."{table}"'


def main() -> None:
    alerts = run_drift_detection()
    print(f"Wrote {len(alerts)} drift alerts")


if __name__ == "__main__":
    main()
