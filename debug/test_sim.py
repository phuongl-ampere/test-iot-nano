import ast
import importlib.util
import pathlib
import sys
import unittest
from unittest.mock import patch


SIM_PATH = pathlib.Path(__file__).with_name("sim.py")


def load_sim_module():
    spec = importlib.util.spec_from_file_location("debug_sim", SIM_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SimScriptTests(unittest.TestCase):
    def test_uses_plain_mqtt_for_the_local_development_broker(self):
        module = ast.parse(SIM_PATH.read_text())
        assignment_names = {
            target.id
            for node in module.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        self.assertEqual(
            next(
                value.value
                for node in module.body
                if isinstance(node, ast.Assign)
                for target in node.targets
                if isinstance(target, ast.Name) and target.id == "MQTT_PORT"
                for value in [node.value]
                if isinstance(value, ast.Constant)
            ),
            1883,
        )
        self.assertNotIn("CA_FILE", assignment_names)
        self.assertFalse(
            any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in {"tls_set", "tls_insecure_set"}
                for node in ast.walk(module)
            )
        )

    def test_builds_consistent_random_measurements(self):
        source = ast.parse(SIM_PATH.read_text())
        measurement_builders = [
            node
            for node in source.body
            if isinstance(node, ast.FunctionDef) and node.name == "next_measurements"
        ]
        self.assertEqual(
            len(measurement_builders),
            1,
            "simulator must define next_measurements",
        )

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

    def test_builds_and_publishes_token_authenticated_telemetry(self):
        module = ast.parse(SIM_PATH.read_text())
        while_loop = next(
            node
            for node in ast.walk(module)
            if isinstance(node, ast.While)
        )

        payload_assignment = next(
            node
            for node in while_loop.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "payload"
                for target in node.targets
            )
        )
        self.assertIsInstance(payload_assignment.value, ast.Dict)
        payload_keys = {
            key.value
            for key in payload_assignment.value.keys
            if isinstance(key, ast.Constant)
        }
        self.assertEqual(
            payload_keys,
            {
                "schema_version",
                "boot_id",
                "sequence",
                "event_at",
                "measurements",
            },
        )

        publish_call = next(
            node
            for node in while_loop.body
            if isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr == "publish"
        )
        self.assertEqual(publish_call.value.args[0].value, "v1/devices/me/telemetry")
        self.assertEqual(publish_call.value.keywords[0].arg, "qos")
        self.assertEqual(publish_call.value.keywords[0].value.value, 1)


if __name__ == "__main__":
    unittest.main()
