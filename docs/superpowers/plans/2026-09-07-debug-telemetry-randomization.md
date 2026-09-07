# Debug Telemetry Randomization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish internally consistent, variable electrical measurements from the Python debug simulator.

**Architecture:** Extract a pure measurement builder that selects random electrical inputs, derives power from them, and advances cumulative energy using the publish interval. Put the simulator loop behind a main guard so the builder can be imported and tested without connecting to MQTT.

**Tech Stack:** Python 3 standard library `random` and `unittest`; Paho MQTT.

## Global Constraints

- Preserve the existing telemetry schema, MQTT topic, QoS 1, token authentication, and plaintext local development broker configuration.
- Voltage, current, frequency, and power factor must remain within plausible operating ranges.
- `power_w` must equal the published voltage, current, and power factor product.
- `energy_kwh` must only increase.

---

### Task 1: Random measurement builder and simulator loop

**Files:**
- Modify: `debug/sim.py`
- Modify: `debug/test_sim.py`

**Interfaces:**
- Produces: `next_measurements(energy_kwh: float, interval_seconds: float) -> tuple[dict[str, float], float]`
- Consumes: the last cumulative energy reading and the 10-second publish interval.

- [x] **Step 1: Write the failing test**

```python
import importlib.util
import sys
from unittest.mock import patch


def load_sim_module():
    spec = importlib.util.spec_from_file_location("debug_sim", SIM_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_builds_consistent_random_measurements(self):
    module = load_sim_module()
    with patch.object(
        module.random,
        "uniform",
        side_effect=[230.0, 2.0, 50.0, 0.95],
    ):
        measurements, energy_kwh = module.next_measurements(31.25, 10)

    self.assertEqual(measurements["voltage_v"], 230.0)
    self.assertEqual(measurements["current_a"], 2.0)
    self.assertEqual(measurements["frequency_hz"], 50.0)
    self.assertEqual(measurements["power_factor"], 0.95)
    self.assertEqual(measurements["power_w"], 437.0)
    self.assertAlmostEqual(energy_kwh, 31.25 + 437.0 * 10 / 3_600_000)
```

- [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest debug/test_sim.py`

Expected: FAIL because `next_measurements` does not exist.

- [x] **Step 3: Write minimal implementation**

```python
def next_measurements(energy_kwh: float, interval_seconds: float) -> tuple[dict[str, float], float]:
    voltage_v = round(random.uniform(220.0, 240.0), 1)
    current_a = round(random.uniform(0.1, 10.0), 2)
    frequency_hz = round(random.uniform(49.8, 50.2), 2)
    power_factor = round(random.uniform(0.8, 1.0), 2)
    power_w = round(voltage_v * current_a * power_factor, 1)
    energy_kwh += power_w * interval_seconds / 3_600_000
    return {
        "voltage_v": voltage_v,
        "current_a": current_a,
        "power_w": power_w,
        "energy_kwh": energy_kwh,
        "frequency_hz": frequency_hz,
        "power_factor": power_factor,
    }, energy_kwh
```

Replace the fixed measurements with the function result and run the publish loop only under `if __name__ == "__main__":`.

- [x] **Step 4: Run test and live simulator verification**

Run: `python3 -m unittest debug/test_sim.py`

Expected: PASS with all tests green.

Run: `PYTHONUNBUFFERED=1 python3 debug/sim.py`

Expected: two successive `Published` payloads with varying measurements and increasing `energy_kwh`.
