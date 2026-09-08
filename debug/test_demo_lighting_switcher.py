import importlib.util
import pathlib
import sys
import unittest
from datetime import datetime


SCRIPT_PATH = pathlib.Path(__file__).with_name("demo_lighting_switcher.py")


def load_module():
    spec = importlib.util.spec_from_file_location("demo_lighting_switcher", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


module = load_module()


class LightingSwitcherTests(unittest.TestCase):
    def test_set_brightness_updates_state_and_two_way_result(self):
        state = module.LightingSwitcherState()
        outcome = state.handle_rpc(
            {
                "id": "command-1",
                "method": "set_brightness",
                "params": {"brightness_pct": 75},
                "mode": "two_way",
            }
        )
        self.assertTrue(outcome.applied)
        self.assertEqual(
            outcome.response,
            {"ok": True, "result": {"switch_state": True, "brightness_pct": 75}},
        )

    def test_off_measurement_has_zero_power_and_unchanged_energy(self):
        state = module.LightingSwitcherState()
        before = state.measurements(10)["energy_kwh"]
        after = state.measurements(10)
        self.assertEqual(after["power_w"], 0.0)
        self.assertEqual(after["energy_kwh"], before)

    def test_brightness_boundaries_control_power_and_switch(self):
        state = module.LightingSwitcherState()

        zero = state.handle_rpc(
            {"method": "set_brightness", "params": {"brightness_pct": 0}}
        )
        self.assertTrue(zero.applied)
        self.assertEqual(state.measurements(0)["switch_state"], False)

        maximum = state.handle_rpc(
            {"method": "set_brightness", "params": {"brightness_pct": 100}}
        )
        self.assertTrue(maximum.applied)
        self.assertEqual(state.measurements(0)["power_w"], 10.0)

    def test_invalid_two_way_request_returns_structured_error_without_change(self):
        state = module.LightingSwitcherState()
        outcome = state.handle_rpc(
            {
                "id": "command-2",
                "method": "set_brightness",
                "params": {"brightness_pct": 101},
                "mode": "two_way",
            }
        )
        self.assertFalse(outcome.applied)
        self.assertEqual(outcome.response, {"ok": False, "error": "unsupported_method"})
        self.assertEqual(state.measurements(0)["brightness_pct"], 0)

    def test_one_way_valid_request_has_no_response(self):
        state = module.LightingSwitcherState()
        outcome = state.handle_rpc(
            {"method": "switch_on", "params": {}, "mode": "one_way"}
        )
        self.assertTrue(outcome.applied)
        self.assertIsNone(outcome.response)

    def test_telemetry_contains_envelope_and_measurements(self):
        state = module.LightingSwitcherState()
        telemetry = module.build_telemetry(state, "boot-1", 7, 10)

        self.assertEqual(telemetry["schema_version"], 1)
        self.assertEqual(telemetry["boot_id"], "boot-1")
        self.assertEqual(telemetry["sequence"], 7)
        self.assertEqual(
            telemetry["measurements"],
            {
                "switch_state": False,
                "brightness_pct": 0,
                "power_w": 0.0,
                "energy_kwh": 0.0,
            },
        )
        parsed = datetime.fromisoformat(telemetry["event_at"].replace("Z", "+00:00"))
        self.assertIsNotNone(parsed.tzinfo)

    def test_rpc_topics_require_one_request_identifier(self):
        self.assertTrue(
            module.is_rpc_request_topic("v1/devices/me/rpc/request/cmd-1")
        )
        self.assertFalse(module.is_rpc_request_topic("v1/devices/me/rpc/request/+"))
        self.assertFalse(
            module.is_rpc_request_topic("v1/devices/me/rpc/request/cmd-1/extra")
        )
        self.assertEqual(
            module.rpc_response_topic("cmd-1"),
            "v1/devices/me/rpc/response/cmd-1",
        )


if __name__ == "__main__":
    unittest.main()
