import importlib.util
import json
import os
import pathlib
import sys
import unittest
from datetime import datetime
from unittest import mock


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


class RecordingPublishInfo:
    def wait_for_publish(self):
        return None


class RecordingMqttClient:
    instances = []

    class ReasonCode:
        is_failure = False

    def __init__(self, *args, **kwargs):
        self.constructor_args = args
        self.constructor_kwargs = kwargs
        self.username = None
        self.tls_calls = []
        self.subscriptions = []
        self.published = []
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False
        self.on_connect = None
        self.on_subscribe = None
        self.on_message = None
        self.__class__.instances.append(self)

    def username_pw_set(self, username, password):
        self.username = (username, password)

    def tls_set(self, **kwargs):
        self.tls_calls.append(kwargs)

    def connect(self, host, port, keepalive):
        self.connect_args = (host, port, keepalive)

    def loop_start(self):
        self.loop_started = True
        self.on_connect(self, None, None, self.ReasonCode(), None)
        self.on_message(
            self,
            None,
            type(
                "Message",
                (),
                {
                    "topic": "v1/devices/me/rpc/request/cmd-1",
                    "payload": json.dumps(
                        {
                            "id": "#invalid",
                            "method": "set_brightness",
                            "params": {"brightness_pct": 75},
                            "mode": "two_way",
                        }
                    ).encode("utf-8"),
                },
            )(),
        )

    def loop_stop(self):
        self.loop_stopped = True

    def disconnect(self):
        self.disconnected = True

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        self.on_subscribe(self, None, 1, [qos], None)

    def publish(self, topic, payload, qos):
        self.published.append((topic, payload, qos))
        return RecordingPublishInfo()


class LightingSwitcherTests(unittest.TestCase):
    def test_configuration_defaults_to_the_local_plain_mqtt_listener(self):
        with mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True):
            configuration = module.configuration_from_environment()

        self.assertEqual(configuration.host, "127.0.0.1")
        self.assertEqual(configuration.port, 1883)
        self.assertIsNone(configuration.ca_file)

    def test_configuration_requires_device_token(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                module.configuration_from_environment()

    def test_configuration_reads_custom_listener_and_optional_tls_settings(self):
        environment = {
            "DEVICE_TOKEN": "test-token",
            "MQTT_HOST": "mqtt.test",
            "MQTT_PORT": "2883",
            "MQTT_CA_FILE": "/tmp/test-ca.pem",
            "TELEMETRY_INTERVAL_SECONDS": "3.5",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            configuration = module.configuration_from_environment()

        self.assertEqual(configuration.host, "mqtt.test")
        self.assertEqual(configuration.port, 2883)
        self.assertEqual(configuration.ca_file, "/tmp/test-ca.pem")
        self.assertEqual(configuration.publish_interval_seconds, 3.5)

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
        self.assertFalse(module.is_rpc_request_topic("v1/devices/me/rpc/request/#"))
        self.assertFalse(
            module.is_rpc_request_topic("v1/devices/me/rpc/request/cmd+1")
        )
        self.assertFalse(
            module.is_rpc_request_topic("v1/devices/me/rpc/request/cmd-1/extra")
        )
        self.assertEqual(
            module.rpc_response_topic("cmd-1"),
            "v1/devices/me/rpc/response/cmd-1",
        )
        with self.assertRaises(ValueError):
            module.rpc_response_topic("#")
        with self.assertRaises(ValueError):
            module.rpc_response_topic("cmd+1")

    def test_decode_rpc_request_uses_concrete_topic_id_over_payload_id(self):
        request = module.decode_rpc_request(
            "v1/devices/me/rpc/request/cmd-1",
            json.dumps({"id": "#invalid", "method": "switch_on"}).encode("utf-8"),
        )

        self.assertEqual(request["id"], "cmd-1")

    def test_main_configures_v2_auth_subscribes_publishes_and_cleans_up(self):
        RecordingMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {"DEVICE_TOKEN": "test-token", "TELEMETRY_INTERVAL_SECONDS": "0"},
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=RecordingMqttClient
            ),
            mock.patch.object(module.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                module.main()

        client = RecordingMqttClient.instances[0]
        self.assertEqual(
            client.constructor_args[0], module.mqtt.CallbackAPIVersion.VERSION2
        )
        self.assertEqual(client.username, ("test-token", ""))
        self.assertEqual(
            client.subscriptions,
            [("v1/devices/me/rpc/request/+", 1)],
        )
        self.assertEqual(client.tls_calls, [])
        self.assertTrue(client.loop_started)
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)
        telemetry = [
            item for item in client.published if item[0] == module.TELEMETRY_TOPIC
        ]
        responses = [
            item
            for item in client.published
            if item[0] == "v1/devices/me/rpc/response/cmd-1"
        ]
        self.assertGreaterEqual(len(telemetry), 1)
        self.assertTrue(all(item[2] == 1 for item in telemetry))
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0][2], 1)
        self.assertEqual(json.loads(responses[0][1])["ok"], True)

    def test_main_enables_tls_only_for_configured_ca_file(self):
        RecordingMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {
                    "DEVICE_TOKEN": "test-token",
                    "MQTT_CA_FILE": "/tmp/test-ca.pem",
                    "TELEMETRY_INTERVAL_SECONDS": "0",
                },
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=RecordingMqttClient
            ),
            mock.patch.object(module.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                module.main()

        self.assertEqual(
            RecordingMqttClient.instances[0].tls_calls,
            [{"ca_certs": "/tmp/test-ca.pem"}],
        )


if __name__ == "__main__":
    unittest.main()
