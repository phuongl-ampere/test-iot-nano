import importlib.util
import json
import logging
import os
import pathlib
import queue
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
        self.publish_infos = []
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
        return (0, 1)

    def publish(self, topic, payload, qos):
        self.published.append((topic, payload, qos))
        info = RecordingPublishInfo()
        self.publish_infos.append(info)
        return info


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


class ReadinessTimeoutMqttClient(RecordingMqttClient):
    def loop_start(self):
        self.loop_started = True


class RejectedReconnectMqttClient(RecordingMqttClient):
    class FailureReasonCode:
        is_failure = True

    def loop_start(self):
        self.loop_started = True
        self.on_connect(self, None, None, self.ReasonCode(), None)


class ImmediateSubscribeFailureMqttClient(RecordingMqttClient):
    def subscribe(self, topic, qos):
        self.subscriptions.append((topic, qos))
        return (1, None)


class CommandResponseFailureMqttClient(RecordingMqttClient):
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
                    "topic": "v1/devices/me/rpc/request/cmd-fail",
                    "payload": json.dumps(
                        {
                            "method": "switch_on",
                            "mode": "two_way",
                        }
                    ).encode("utf-8"),
                },
            )(),
        )

    def publish(self, topic, payload, qos):
        self.published.append((topic, payload, qos))
        if topic.startswith(module.RPC_RESPONSE_PREFIX):
            info = RecordingPublishInfo(rc=1, published=False)
        else:
            info = RecordingPublishInfo()
        self.publish_infos.append(info)
        return info


class RapidCommandMqttClient(RecordingMqttClient):
    def loop_start(self):
        self.loop_started = True
        self.on_connect(self, None, None, self.ReasonCode(), None)
        for request_id, method in (("first", "switch_on"), ("second", "switch_off")):
            self.on_message(
                self,
                None,
                type(
                    "Message",
                    (),
                    {
                        "topic": f"v1/devices/me/rpc/request/{request_id}",
                        "payload": json.dumps({"method": method}).encode("utf-8"),
                    },
                )(),
            )


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

    def test_publish_ack_timeout_is_bounded_and_detected(self):
        self.assertGreater(module.PUBLISH_ACK_TIMEOUT_SECONDS, 0)
        info = RecordingPublishInfo(published=False)
        client = mock.Mock()
        client.publish.return_value = info

        with self.assertRaisesRegex(RuntimeError, "telemetry publish"):
            module.publish_qos1(
                client,
                module.TELEMETRY_TOPIC,
                "{}",
                description="telemetry",
            )

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

    def test_rpc_callback_enqueues_without_waiting_for_a_publish_ack(self):
        requests = queue.Queue()
        publish_started = threading.Event()
        release_publish = threading.Event()

        class BlockingPublishInfo:
            rc = 0

            def wait_for_publish(self, timeout):
                publish_started.set()
                release_publish.wait(1)

            def is_published(self):
                return True

        client = mock.Mock()
        client.publish.return_value = BlockingPublishInfo()
        publish_thread = threading.Thread(
            target=module.publish_qos1,
            args=(client, module.TELEMETRY_TOPIC, "{}"),
            kwargs={"description": "telemetry"},
        )
        publish_thread.start()
        self.assertTrue(publish_started.wait(1))

        callback_thread = threading.Thread(
            target=module.enqueue_rpc_request,
            args=(
                requests,
                "v1/devices/me/rpc/request/cmd-1",
                json.dumps({"method": "switch_on"}).encode("utf-8"),
            ),
        )
        callback_thread.start()
        callback_thread.join(1)
        release_publish.set()
        publish_thread.join(1)

        self.assertFalse(callback_thread.is_alive())
        self.assertEqual(requests.get_nowait()["id"], "cmd-1")

    def test_full_request_queue_drops_without_blocking_or_logging_payload(self):
        requests = queue.Queue(maxsize=1)
        first_payload = json.dumps({"method": "switch_on"}).encode("utf-8")
        dropped_payload = b'{"method":"switch_off","secret":"device-token"}'
        self.assertTrue(
            module.enqueue_rpc_request(
                requests,
                "v1/devices/me/rpc/request/accepted",
                first_payload,
            )
        )

        result = []
        callback_thread = threading.Thread(
            target=lambda: result.append(
                module.enqueue_rpc_request(
                    requests,
                    "v1/devices/me/rpc/request/dropped",
                    dropped_payload,
                )
            ),
            daemon=True,
        )
        with self.assertLogs(module.LOGGER, level="WARNING") as logs:
            callback_thread.start()
            callback_thread.join(1)

        self.assertFalse(callback_thread.is_alive())
        self.assertEqual(result, [False])
        self.assertEqual(requests.qsize(), 1)
        self.assertEqual(requests.get_nowait()["id"], "accepted")
        log_output = "\n".join(logs.output)
        self.assertIn("request queue full", log_output)
        self.assertNotIn(dropped_payload.decode("utf-8"), log_output)
        self.assertNotIn("device-token", log_output)

    def test_main_worker_processes_command_and_ack_checks_two_way_response(self):
        RecordingMqttClient.instances = []
        with (
            mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True),
            mock.patch.object(
                module.mqtt, "Client", side_effect=RecordingMqttClient
            ),
            mock.patch.object(module.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            module.main()

        client = RecordingMqttClient.instances[0]
        response_info = next(
            info
            for info, published in zip(client.publish_infos, client.published)
            if published[0] == module.RPC_RESPONSE_PREFIX + "cmd-1"
        )
        self.assertEqual(
            response_info.wait_timeout,
            module.PUBLISH_ACK_TIMEOUT_SECONDS,
        )
        telemetry_sequences = [
            json.loads(payload)["sequence"]
            for topic, payload, _qos in client.published
            if topic == module.TELEMETRY_TOPIC
        ]
        self.assertEqual(telemetry_sequences, [0, 1])

    def test_main_stops_after_rejected_reconnect_and_cleans_up(self):
        RejectedReconnectMqttClient.instances = []
        sleep_calls = 0

        def trigger_rejected_reconnect(_seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls == 1:
                client = RejectedReconnectMqttClient.instances[0]
                client.on_connect(
                    client,
                    None,
                    None,
                    client.FailureReasonCode(),
                    None,
                )
                return
            raise KeyboardInterrupt

        with (
            mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True),
            mock.patch.object(
                module.mqtt,
                "Client",
                side_effect=RejectedReconnectMqttClient,
            ),
            mock.patch.object(
                module.time,
                "sleep",
                side_effect=trigger_rejected_reconnect,
            ),
        ):
            with self.assertRaises(SystemExit):
                module.main()

        client = RejectedReconnectMqttClient.instances[0]
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)

    def test_command_publish_failure_exits_and_cleans_up_from_main_worker(self):
        CommandResponseFailureMqttClient.instances = []
        with (
            mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True),
            mock.patch.object(
                module.mqtt,
                "Client",
                side_effect=CommandResponseFailureMqttClient,
            ),
        ):
            with self.assertRaises(SystemExit):
                module.main()

        client = CommandResponseFailureMqttClient.instances[0]
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)

    def test_rapid_commands_account_energy_using_real_elapsed_seconds(self):
        RapidCommandMqttClient.instances = []
        monotonic_values = iter((100.0, 101.0, 101.1, 101.2))
        with (
            mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True),
            mock.patch.object(
                module.mqtt, "Client", side_effect=RapidCommandMqttClient
            ),
            mock.patch.object(
                module.time, "monotonic", side_effect=lambda: next(monotonic_values)
            ),
            mock.patch.object(module.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            module.main()

        client = RapidCommandMqttClient.instances[0]
        telemetry = [
            json.loads(payload)
            for topic, payload, _qos in client.published
            if topic == module.TELEMETRY_TOPIC
        ]
        self.assertEqual(len(telemetry), 3)
        self.assertEqual(telemetry[-1]["measurements"]["energy_kwh"], 0.000000278)

    def test_immediate_subscribe_failure_skips_readiness_wait_and_cleans_up(self):
        ImmediateSubscribeFailureMqttClient.instances = []
        with (
            mock.patch.dict(os.environ, {"DEVICE_TOKEN": "test-token"}, clear=True),
            mock.patch.object(
                module.mqtt,
                "Client",
                side_effect=ImmediateSubscribeFailureMqttClient,
            ),
            mock.patch.object(module.threading.Event, "wait") as wait,
        ):
            with self.assertRaises(SystemExit):
                module.main()

        wait.assert_not_called()
        client = ImmediateSubscribeFailureMqttClient.instances[0]
        self.assertTrue(client.loop_stopped)
        self.assertTrue(client.disconnected)

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
