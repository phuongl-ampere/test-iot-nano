import importlib.util
import json
import logging
import os
import pathlib
import sys
import threading
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
    def __init__(self, rc=0, published=True):
        self.rc = rc
        self._published = published
        self.wait_timeout = None

    def wait_for_publish(self, timeout=None):
        self.wait_timeout = timeout
        return None

    def is_published(self):
        return self._published


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


class DowngradedSubscriptionMqttClient(RecordingMqttClient):
    def loop_start(self):
        self.loop_started = True
        self.on_connect(self, None, None, self.ReasonCode(), None)

    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        self.on_subscribe(self, None, 1, [0], None)


class FailedPublishMqttClient(RecordingMqttClient):
    def loop_start(self):
        self.loop_started = True
        self.on_connect(self, None, None, self.ReasonCode(), None)

    def publish(self, topic, payload, qos):
        self.published.append((topic, payload, qos))
        return RecordingPublishInfo(rc=1, published=False)


class TimeoutPublishMqttClient(RecordingMqttClient):
    def loop_start(self):
        self.loop_started = True
        self.on_connect(self, None, None, self.ReasonCode(), None)

    def publish(self, topic, payload, qos):
        self.published.append((topic, payload, qos))
        return RecordingPublishInfo(rc=0, published=False)


class OrderedBlockingMqttClient:
    def __init__(self):
        self.completed_sequences = []
        self.first_publish_started = threading.Event()
        self.release_first_publish = threading.Event()
        self._publish_count = 0
        self._lock = threading.Lock()

    def publish(self, _topic, payload, qos):
        self.assert_qos = qos
        sequence = json.loads(payload)["sequence"]
        with self._lock:
            self._publish_count += 1
            publish_number = self._publish_count
        if publish_number == 1:
            self.first_publish_started.set()
            self.release_first_publish.wait(2)
        with self._lock:
            self.completed_sequences.append(sequence)
        return RecordingPublishInfo()


class ReadinessTimeoutMqttClient(RecordingMqttClient):
    def loop_start(self):
        self.loop_started = True


class LightingSwitcherTests(unittest.TestCase):
    def test_configure_logging_bootstraps_info_with_stable_format(self):
        with mock.patch.object(logging, "basicConfig") as basic_config:
            module.configure_logging()

        basic_config.assert_called_once_with(
            level=logging.INFO,
            format="%(levelname)s:%(name)s:%(message)s",
        )

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
            "PUBLISH_INTERVAL_SECONDS": "3.5",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            configuration = module.configuration_from_environment()

        self.assertEqual(configuration.host, "mqtt.test")
        self.assertEqual(configuration.port, 2883)
        self.assertEqual(configuration.ca_file, "/tmp/test-ca.pem")
        self.assertEqual(configuration.publish_interval_seconds, 3.5)

    def test_configuration_rejects_non_positive_or_non_finite_intervals(self):
        for value in ("0", "-1", "nan", "inf", "-inf"):
            with self.subTest(value=value):
                environment = {
                    "DEVICE_TOKEN": "test-token",
                    "PUBLISH_INTERVAL_SECONDS": value,
                }
                with mock.patch.dict(os.environ, environment, clear=True):
                    with self.assertRaises(ValueError):
                        module.configuration_from_environment()

    def test_configuration_accepts_a_finite_positive_custom_interval(self):
        with mock.patch.dict(
            os.environ,
            {"DEVICE_TOKEN": "test-token", "PUBLISH_INTERVAL_SECONDS": "3.5"},
            clear=True,
        ):
            configuration = module.configuration_from_environment()

        self.assertEqual(configuration.publish_interval_seconds, 3.5)

    def test_configuration_ignores_removed_telemetry_interval_alias(self):
        with mock.patch.dict(
            os.environ,
            {
                "DEVICE_TOKEN": "test-token",
                "TELEMETRY_INTERVAL_SECONDS": "0",
            },
            clear=True,
        ):
            configuration = module.configuration_from_environment()

        self.assertEqual(configuration.publish_interval_seconds, 10)

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

    def test_invalid_explicit_rpc_mode_is_ignored_without_state_change(self):
        state = module.LightingSwitcherState()
        outcome = state.handle_rpc(
            {"method": "switch_on", "params": {}, "mode": "invalid"}
        )

        self.assertFalse(outcome.applied)
        self.assertEqual(state.measurements(0)["switch_state"], False)

    def test_decode_ignores_invalid_explicit_rpc_mode(self):
        request = module.decode_rpc_request(
            "v1/devices/me/rpc/request/cmd-1",
            json.dumps(
                {"method": "switch_on", "mode": "invalid"}
            ).encode("utf-8"),
        )

        self.assertIsNone(request)

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

    def test_telemetry_publishes_unique_sequences_in_completion_order(self):
        client = OrderedBlockingMqttClient()
        publisher = module.TelemetryPublisher(
            client, module.LightingSwitcherState(), 10
        )
        first = threading.Thread(
            target=publisher.publish, kwargs={"wait_for_ack": False}
        )
        second = threading.Thread(
            target=publisher.publish, kwargs={"wait_for_ack": False}
        )

        first.start()
        self.assertTrue(client.first_publish_started.wait(1))
        second.start()
        client.release_first_publish.set()
        first.join(1)
        second.join(1)

        self.assertEqual(client.completed_sequences, [0, 1])

    def test_publish_ack_timeout_is_bounded_and_detected(self):
        self.assertGreater(module.PUBLISH_ACK_TIMEOUT_SECONDS, 0)
        info = RecordingPublishInfo(published=False)
        client = mock.Mock()
        client.publish.return_value = info
        publisher = module.TelemetryPublisher(
            client, module.LightingSwitcherState(), 10
        )

        with self.assertRaisesRegex(RuntimeError, "telemetry publish"):
            publisher.publish()

        self.assertEqual(info.wait_timeout, module.PUBLISH_ACK_TIMEOUT_SECONDS)

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

    def test_subscription_requires_every_grant_to_be_exactly_qos_one(self):
        self.assertTrue(module.subscription_granted([1]))
        self.assertFalse(module.subscription_granted([]))
        self.assertFalse(module.subscription_granted([0]))
        self.assertFalse(module.subscription_granted([1, 2]))

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
                {"DEVICE_TOKEN": "test-token", "PUBLISH_INTERVAL_SECONDS": "10"},
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=RecordingMqttClient
            ),
            mock.patch.object(module.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertLogs(module.LOGGER, level="INFO") as logs:
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
        log_output = "\n".join(logs.output)
        self.assertIn("connected", log_output)
        self.assertIn("telemetry sequence=0", log_output)
        self.assertNotIn("test-token", log_output)

    def test_main_enables_tls_only_for_configured_ca_file(self):
        RecordingMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {
                    "DEVICE_TOKEN": "test-token",
                    "MQTT_CA_FILE": "/tmp/test-ca.pem",
                    "PUBLISH_INTERVAL_SECONDS": "10",
                },
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=RecordingMqttClient
            ),
            mock.patch.object(module.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            module.main()

        self.assertEqual(
            RecordingMqttClient.instances[0].tls_calls,
            [{"ca_certs": "/tmp/test-ca.pem"}],
        )

    def test_main_rejects_downgraded_subscription_and_cleans_up(self):
        DowngradedSubscriptionMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {"DEVICE_TOKEN": "test-token"},
                clear=True,
            ),
            mock.patch.object(
                module.mqtt,
                "Client",
                side_effect=DowngradedSubscriptionMqttClient,
            ),
            mock.patch.object(
                module.threading.Event,
                "wait",
                side_effect=[True, False],
            ),
        ):
            with self.assertRaises(SystemExit):
                module.main()

        client = DowngradedSubscriptionMqttClient.instances[0]
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)

    def test_main_publish_failure_exits_and_cleans_up(self):
        FailedPublishMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {"DEVICE_TOKEN": "test-token"},
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=FailedPublishMqttClient
            ),
        ):
            with self.assertRaises(SystemExit):
                module.main()

        client = FailedPublishMqttClient.instances[0]
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)

    def test_main_publish_timeout_exits_and_cleans_up(self):
        TimeoutPublishMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {"DEVICE_TOKEN": "test-token"},
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=TimeoutPublishMqttClient
            ),
        ):
            with self.assertRaises(SystemExit):
                module.main()

        client = TimeoutPublishMqttClient.instances[0]
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)

    def test_main_readiness_timeout_stops_loop_and_disconnects(self):
        ReadinessTimeoutMqttClient.instances = []
        with (
            mock.patch.dict(
                os.environ,
                {"DEVICE_TOKEN": "test-token"},
                clear=True,
            ),
            mock.patch.object(
                module.mqtt, "Client", side_effect=ReadinessTimeoutMqttClient
            ),
            mock.patch.object(module.threading.Event, "wait", return_value=False),
        ):
            with self.assertRaises(SystemExit):
                module.main()

        client = ReadinessTimeoutMqttClient.instances[0]
        self.assertTrue(client.loop_started)
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)


if __name__ == "__main__":
    unittest.main()
