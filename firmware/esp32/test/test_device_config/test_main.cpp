#include <unity.h>

#include <vector>

#include <device_config.h>
#include <one_way_rpc.h>
#include <uuid_v4.h>

namespace {

constexpr time_t kRpcNow = 1788739230;  // 2026-09-07T00:00:30Z

iot::DeviceConfig valid_config() {
  iot::DeviceConfig config;
  config.wifi_ssid = "lab-wifi";
  config.mqtt_host = "broker.local";
  config.mqtt_port = 8883;
  config.device_token = "device-token";
  config.mqtt_ca_pem = "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n";
  return config;
}

std::string valid_rpc_command(const char* id, const char* method,
                              const char* expires_at = "2026-09-07T00:01:00Z",
                              const char* mode = nullptr) {
  return std::string("{\"id\":\"") + id + "\",\"method\":\"" + method +
         "\",\"params\":{},\"issued_at\":\"2026-09-07T00:00:00Z\",\"expires_at\":\"" +
         expires_at + "\"" + (mode == nullptr ? "" : std::string(",\"mode\":\"") + mode + "\"") +
         "}";
}

class RecordingRpcActions final : public iot::OneWayRpcActions {
 public:
  void sample_now() override {
    ++sample_now_calls;
  }

  void reboot() override {
    ++reboot_calls;
  }

  void publish_two_way_response(const std::string& id, const std::string& response) override {
    response_ids.push_back(id);
    responses.push_back(response);
  }

  int sample_now_calls = 0;
  int reboot_calls = 0;
  std::vector<std::string> response_ids;
  std::vector<std::string> responses;
};

}  // namespace

void test_static_network_requires_all_ipv4_fields() {
  iot::DeviceConfig config = valid_config();
  config.mqtt_host = "192.168.1.10";
  config.use_dhcp = false;
  config.static_ip = "192.168.1.123";
  config.gateway = "192.168.1.1";
  config.subnet = "255.255.255.0";
  config.dns = "192.168.1.1";

  TEST_ASSERT_TRUE(config.is_valid());

  config.gateway = "not-an-ip";
  TEST_ASSERT_FALSE(config.is_valid());
}

void test_dhcp_configuration_does_not_require_static_addresses() {
  iot::DeviceConfig config = valid_config();
  config.use_dhcp = true;

  TEST_ASSERT_TRUE(config.is_valid());
}

void test_device_token_is_required() {
  iot::DeviceConfig config = valid_config();
  config.device_token.clear();

  TEST_ASSERT_FALSE(config.is_valid());
}

void test_mqtt_ca_pem_is_required() {
  iot::DeviceConfig config = valid_config();
  config.mqtt_ca_pem.clear();

  TEST_ASSERT_FALSE(config.is_valid());
}

void test_only_tls_port_8883_is_accepted() {
  iot::DeviceConfig config = valid_config();
  config.mqtt_port = 1883;

  TEST_ASSERT_FALSE(config.is_valid());
}

void test_telemetry_topic_is_the_token_only_endpoint() {
  TEST_ASSERT_EQUAL_STRING("v1/devices/me/telemetry", iot::telemetry_topic().c_str());
}

void test_telemetry_qos_is_one() {
  TEST_ASSERT_EQUAL_INT(1, iot::telemetry_qos());
}

void test_one_way_rpc_subscription_uses_thingsboard_request_topic_at_qos_one() {
  TEST_ASSERT_EQUAL_STRING("v1/devices/me/rpc/request/+",
                           iot::one_way_rpc_request_topic().c_str());
  TEST_ASSERT_EQUAL_INT(1, iot::one_way_rpc_qos());
}

void test_gateway_one_way_rpc_subscription_uses_gateway_request_topic_at_qos_one() {
  TEST_ASSERT_EQUAL_STRING("v1/gateways/me/rpc/request/+",
                           iot::one_way_gateway_rpc_request_topic().c_str());
  TEST_ASSERT_EQUAL_INT(1, iot::one_way_rpc_qos());
}

void test_one_way_rpc_matches_direct_and_gateway_request_instances() {
  TEST_ASSERT_TRUE(iot::matches_one_way_rpc_request_topic(
      "v1/devices/me/rpc/request/018f6da9-1234-7abc-8def-0123456789ab"));
  TEST_ASSERT_TRUE(iot::matches_one_way_rpc_request_topic(
      "v1/gateways/me/rpc/request/018f6da9-1234-7abc-8def-0123456789ab"));
  TEST_ASSERT_FALSE(iot::matches_one_way_rpc_request_topic(
      "v1/devices/me/rpc/request/+"));
  TEST_ASSERT_FALSE(iot::matches_one_way_rpc_request_topic(
      "v1/devices/me/rpc/request/"));
}

void test_two_way_response_topic_matches_the_authenticated_virtual_request_role() {
  TEST_ASSERT_EQUAL_STRING("v1/devices/me/rpc/response/018f6da9-1234-7abc-8def-0123456789ab",
                           iot::rpc_response_topic(
                               "018f6da9-1234-7abc-8def-0123456789ab", false)
                               .c_str());
  TEST_ASSERT_EQUAL_STRING("v1/gateways/me/rpc/response/018f6da9-1234-7abc-8def-0123456789ab",
                           iot::rpc_response_topic(
                               "018f6da9-1234-7abc-8def-0123456789ab", true)
                               .c_str());
}

void test_sample_now_rpc_triggers_an_immediate_sample() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;

  const iot::OneWayRpcResult result = processor.handle(
      valid_rpc_command("018f6da9-1234-7abc-8def-0123456789ab", "sample_now"), kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::Executed),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(1, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(0, actions.reboot_calls);
}

void test_duplicate_uuid_v7_rpc_is_not_executed_twice() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;
  const std::string command =
      valid_rpc_command("018f6da9-1234-7abc-8def-0123456789ab", "sample_now");

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::Executed),
                        static_cast<int>(processor.handle(command, kRpcNow, actions)));
  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::IgnoredDuplicate),
                        static_cast<int>(processor.handle(command, kRpcNow, actions)));
  TEST_ASSERT_EQUAL_INT(1, actions.sample_now_calls);
}

void test_malformed_rpc_is_rejected_without_action() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;

  const iot::OneWayRpcResult result =
      processor.handle("{\"id\":\"not-a-uuid\"", kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::IgnoredMalformed),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(0, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(0, actions.reboot_calls);
}

void test_expired_rpc_is_rejected_without_action() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;

  const iot::OneWayRpcResult result = processor.handle(
      valid_rpc_command("018f6da9-1234-7abc-8def-0123456789ac", "sample_now",
                        "2026-09-07T00:00:29Z"),
      kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::IgnoredExpired),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(0, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(0, actions.reboot_calls);
}

void test_reboot_rpc_invokes_the_reboot_action_without_a_response_callback() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;

  const iot::OneWayRpcResult result =
      processor.handle(valid_rpc_command("018f6da9-1234-7abc-8def-0123456789ad", "reboot"),
                       kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::Executed),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(0, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(1, actions.reboot_calls);
}

void test_two_way_sample_now_returns_a_response_after_triggering_the_sample() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;
  const char* id = "018f6da9-1234-7abc-8def-0123456789af";

  const iot::OneWayRpcResult result =
      processor.handle(valid_rpc_command(id, "sample_now", "2026-09-07T00:01:00Z", "two_way"),
                       kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::Executed),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(1, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(0, actions.reboot_calls);
  TEST_ASSERT_EQUAL_INT(1, actions.responses.size());
  TEST_ASSERT_EQUAL_STRING(id, actions.response_ids[0].c_str());
  TEST_ASSERT_EQUAL_STRING("{\"ok\":true,\"result\":{\"sampled\":true}}",
                           actions.responses[0].c_str());
}

void test_two_way_reboot_is_rejected_without_restarting_the_device() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;
  const char* id = "018f6da9-1234-7abc-8def-0123456789b0";

  const iot::OneWayRpcResult result =
      processor.handle(valid_rpc_command(id, "reboot", "2026-09-07T00:01:00Z", "two_way"),
                       kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::RejectedTwoWay),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(0, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(0, actions.reboot_calls);
  TEST_ASSERT_EQUAL_INT(1, actions.responses.size());
  TEST_ASSERT_EQUAL_STRING("{\"ok\":false,\"error\":\"two_way_reboot_unsupported\"}",
                           actions.responses[0].c_str());
}

void test_generic_firmware_does_not_execute_gateway_child_envelopes() {
  iot::OneWayRpcProcessor processor;
  RecordingRpcActions actions;

  const iot::OneWayRpcResult result = processor.handle(
      valid_rpc_command("018f6da9-1234-7abc-8def-0123456789ae", "gateway_child_rpc"),
      kRpcNow, actions);

  TEST_ASSERT_EQUAL_INT(static_cast<int>(iot::OneWayRpcResult::IgnoredUnsupported),
                        static_cast<int>(result));
  TEST_ASSERT_EQUAL_INT(0, actions.sample_now_calls);
  TEST_ASSERT_EQUAL_INT(0, actions.reboot_calls);
}

void test_uuid_v4_uses_canonical_version_and_variant_bits() {
  const std::string uuid =
      iot::uuid_v4(0x00112233, 0x44556677, 0xff99aabb, 0xccddeeff);

  TEST_ASSERT_EQUAL_STRING("00112233-4455-4677-bf99-aabbccddeeff", uuid.c_str());
}

int main(int, char**) {
  UNITY_BEGIN();
  RUN_TEST(test_static_network_requires_all_ipv4_fields);
  RUN_TEST(test_dhcp_configuration_does_not_require_static_addresses);
  RUN_TEST(test_device_token_is_required);
  RUN_TEST(test_mqtt_ca_pem_is_required);
  RUN_TEST(test_only_tls_port_8883_is_accepted);
  RUN_TEST(test_telemetry_topic_is_the_token_only_endpoint);
  RUN_TEST(test_telemetry_qos_is_one);
  RUN_TEST(test_one_way_rpc_subscription_uses_thingsboard_request_topic_at_qos_one);
  RUN_TEST(test_gateway_one_way_rpc_subscription_uses_gateway_request_topic_at_qos_one);
  RUN_TEST(test_one_way_rpc_matches_direct_and_gateway_request_instances);
  RUN_TEST(test_two_way_response_topic_matches_the_authenticated_virtual_request_role);
  RUN_TEST(test_sample_now_rpc_triggers_an_immediate_sample);
  RUN_TEST(test_duplicate_uuid_v7_rpc_is_not_executed_twice);
  RUN_TEST(test_malformed_rpc_is_rejected_without_action);
  RUN_TEST(test_expired_rpc_is_rejected_without_action);
  RUN_TEST(test_reboot_rpc_invokes_the_reboot_action_without_a_response_callback);
  RUN_TEST(test_two_way_sample_now_returns_a_response_after_triggering_the_sample);
  RUN_TEST(test_two_way_reboot_is_rejected_without_restarting_the_device);
  RUN_TEST(test_generic_firmware_does_not_execute_gateway_child_envelopes);
  RUN_TEST(test_uuid_v4_uses_canonical_version_and_variant_bits);
  return UNITY_END();
}
