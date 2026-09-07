#pragma once

#include <string>

namespace iot {

inline bool is_valid_ipv4(const std::string& value) {
  int segments = 0;
  std::size_t start = 0;

  while (start < value.size()) {
    const std::size_t end = value.find('.', start);
    const std::size_t length = (end == std::string::npos ? value.size() : end) - start;
    if (length == 0 || length > 3) {
      return false;
    }

    int number = 0;
    for (std::size_t index = start; index < start + length; ++index) {
      const char character = value[index];
      if (character < '0' || character > '9') {
        return false;
      }
      number = number * 10 + (character - '0');
    }
    if (number > 255) {
      return false;
    }

    ++segments;
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }

  return segments == 4;
}

struct DeviceConfig {
  std::string wifi_ssid;
  std::string wifi_password;
  bool use_dhcp = true;
  std::string static_ip;
  std::string gateway;
  std::string subnet;
  std::string dns;
  std::string mqtt_host;
  unsigned short mqtt_port = 8883;
  std::string device_token;
  std::string mqtt_ca_pem;
  unsigned long telemetry_interval_ms = 6000;

  bool is_valid() const {
    if (wifi_ssid.empty() || mqtt_host.empty() || mqtt_port != 8883 || device_token.empty() ||
        mqtt_ca_pem.empty() || telemetry_interval_ms == 0) {
      return false;
    }

    if (use_dhcp) {
      return true;
    }

    return is_valid_ipv4(static_ip) && is_valid_ipv4(gateway) && is_valid_ipv4(subnet) &&
           is_valid_ipv4(dns);
  }
};

inline std::string telemetry_topic() {
  return "v1/devices/me/telemetry";
}

inline constexpr int telemetry_qos() {
  return 1;
}

}  // namespace iot
