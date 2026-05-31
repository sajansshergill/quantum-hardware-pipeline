import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generator"))

from calibration_producer import generate_calibration_batch
from health_producer import generate_health_batch
from jobs_producer import generate_jobs_batch
from qpu_devices import DEVICE_REGISTRY
from telemetry_producer import generate_telemetry_batch


def test_telemetry_batch_respects_physical_constraints():
    records = [record for record, _ in generate_telemetry_batch()]

    assert len(records) == sum(device.num_qubits for device in DEVICE_REGISTRY)
    assert all(record["T2_us"] <= 2 * record["T1_us"] + 1e-6 for record in records)
    assert all(record["gate_error_2q"] >= record["gate_error_1q"] for record in records)
    assert all(0.5 <= record["readout_fidelity"] <= 1.0 for record in records)


def test_calibration_jobs_and_health_batches_have_expected_shapes():
    calibration = [record for record, _ in generate_calibration_batch()]
    jobs = [record for record, _ in generate_jobs_batch(jobs_per_device=2)]
    health = [record for record, _ in generate_health_batch()]

    assert len(calibration) == len(DEVICE_REGISTRY)
    assert len(jobs) == len(DEVICE_REGISTRY) * 2
    assert len(health) == len(DEVICE_REGISTRY)
    assert all(record["qubits_calibrated"] <= record["qubits_total"] for record in calibration)
    assert all(abs(record["total_time_seconds"] - (record["queue_wait_seconds"] + record["exec_time_seconds"])) <= 0.05 for record in jobs)
    assert all(0.0 <= record["system_health_score"] <= 1.0 for record in health)
