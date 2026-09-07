#include <unity.h>

#include <device_config.h>
#include <uuid_v4.h>

namespace {

iot::DeviceConfig valid_config() {
  iot::DeviceConfig config;
  config.wifi_ssid = "lab-wifi";
  config.mqtt_host = "broker.local";
  config.mqtt_port = 8883;
  config.device_token = "device-token";
  config.mqtt_ca_pem = "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----\n";
  return config;
}

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
  RUN_TEST(test_uuid_v4_uses_canonical_version_and_variant_bits);
  return UNITY_END();
}
