import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from detection.drift_detector import compute_drift_alerts


def test_compute_drift_alerts_flags_gate_error_spike():
    rows = []
    for i in range(8):
        rows.append(
            {
                "device_id": "ibm_fez",
                "qubit_id": 0,
                "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=i),
                "gate_error_1q": 0.001 + i * 0.00001,
                "gate_error_2q": 0.006 + i * 0.00002,
            }
        )
    rows.append(
        {
            "device_id": "ibm_fez",
            "qubit_id": 0,
            "timestamp": pd.Timestamp("2024-01-01") + pd.Timedelta(minutes=9),
            "gate_error_1q": 0.01,
            "gate_error_2q": 0.06,
        }
    )

    alerts = compute_drift_alerts(pd.DataFrame(rows), window=8, threshold=2.5, min_history=5)

    assert {alert.metric for alert in alerts} == {"gate_error_1q", "gate_error_2q"}
    assert all(alert.device_id == "ibm_fez" for alert in alerts)
