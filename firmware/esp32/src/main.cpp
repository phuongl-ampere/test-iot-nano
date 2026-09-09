#include <Arduino.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <WebServer.h>
#include <WiFi.h>

#include <mqtt_client.h>

#include <cstddef>
#include <cstring>
#include <ctime>

#include "device_config.h"
#include "one_way_rpc.h"
#include "uuid_v4.h"

namespace {

constexpr unsigned long kMqttReconnectDelayMs = 5000;
constexpr unsigned long kProvisioningFallbackMs = 30000;
constexpr unsigned long kDefaultTelemetryIntervalMs = 6000;
constexpr unsigned short kMqttTlsPort = 8883;
constexpr int kProvisionButtonPin = 0;
constexpr int kAdcPin = 34;
constexpr std::size_t kMaximumRpcPayloadBytes = 1024;
constexpr const char* kProvisioningUsername = "admin";
constexpr const char* kProvisioningPassword = PROVISIONING_PASSWORD;
constexpr time_t kMinimumValidEpoch = 1704067200;  // 2024-01-01T00:00:00Z

esp_mqtt_client_handle_t mqtt_client = nullptr;
Preferences preferences;
WebServer config_server(80);
iot::DeviceConfig config;

bool mqtt_connected = false;
bool mqtt_started = false;
bool provisioning = false;
unsigned long wifi_connect_started_at = 0;
unsigned long mqtt_retry_at = 0;
unsigned long next_publish_at = 0;
unsigned long sequence = 0;
String boot_id;
String mqtt_client_id;
iot::OneWayRpcProcessor rpc_processor;
String rpc_payload;
int rpc_payload_total_length = 0;
bool rpc_payload_active = false;
bool rpc_response_to_gateway = false;

String as_string(const std::string& value) {
  return String(value.c_str());
}

std::string as_std_string(const String& value) {
  return std::string(value.c_str());
}

String html_escape(const String& value) {
  String escaped = value;
  escaped.replace("&", "&amp;");
  escaped.replace("<", "&lt;");
  escaped.replace(">", "&gt;");
  escaped.replace("\"", "&quot;");
  return escaped;
}

String iso_timestamp() {
  struct tm timestamp {};
  if (!getLocalTime(&timestamp, 20)) {
    return "1970-01-01T00:00:00Z";
  }

  char buffer[25];
  strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &timestamp);
  return String(buffer);
}

bool read_config() {
  preferences.begin("iot-config", true);
  config.wifi_ssid = as_std_string(preferences.getString("wifi_ssid", ""));
  config.wifi_password = as_std_string(preferences.getString("wifi_password", ""));
  config.use_dhcp = preferences.getBool("use_dhcp", true);
  config.static_ip = as_std_string(preferences.getString("static_ip", ""));
  config.gateway = as_std_string(preferences.getString("gateway", ""));
  config.subnet = as_std_string(preferences.getString("subnet", ""));
  config.dns = as_std_string(preferences.getString("dns", ""));
  config.mqtt_host = as_std_string(preferences.getString("mqtt_host", ""));
  config.mqtt_port = preferences.getUShort("mqtt_port", kMqttTlsPort);
  config.device_token = as_std_string(preferences.getString("device_token", ""));
  config.mqtt_ca_pem = as_std_string(preferences.getString("mqtt_ca_pem", ""));
  config.telemetry_interval_ms =
      preferences.getULong("telemetry_interval_ms", kDefaultTelemetryIntervalMs);
  preferences.end();

  return config.is_valid();
}

void restore_last_processed_rpc_id() {
  preferences.begin("iot-config", true);
  rpc_processor.restore_processed_id(as_std_string(preferences.getString("last_rpc_id", "")));
  preferences.end();
}

bool persist_last_processed_rpc_id() {
  const std::string id = rpc_processor.latest_processed_id();
  if (id.empty()) {
    return false;
  }

  preferences.begin("iot-config", false);
  const std::size_t written = preferences.putString("last_rpc_id", as_string(id));
  preferences.end();
  return written == id.size();
}

void save_config(const iot::DeviceConfig& next_config) {
  preferences.begin("iot-config", false);
  preferences.putString("wifi_ssid", as_string(next_config.wifi_ssid));
  preferences.putString("wifi_password", as_string(next_config.wifi_password));
  preferences.putBool("use_dhcp", next_config.use_dhcp);
  preferences.putString("static_ip", as_string(next_config.static_ip));
  preferences.putString("gateway", as_string(next_config.gateway));
  preferences.putString("subnet", as_string(next_config.subnet));
  preferences.putString("dns", as_string(next_config.dns));
  preferences.putString("mqtt_host", as_string(next_config.mqtt_host));
  preferences.putUShort("mqtt_port", next_config.mqtt_port);
  preferences.putString("device_token", as_string(next_config.device_token));
  preferences.putString("mqtt_ca_pem", as_string(next_config.mqtt_ca_pem));
  preferences.putULong("telemetry_interval_ms", next_config.telemetry_interval_ms);
  preferences.remove("device_id");
  preferences.remove("mqtt_username");
  preferences.remove("mqtt_password");
  preferences.remove("mqtt_tls");
  preferences.remove("last_rpc_id");
  preferences.end();
}

void clear_config() {
  preferences.begin("iot-config", false);
  preferences.clear();
  preferences.end();
}

void render_config_form() {
  if (!config_server.authenticate(kProvisioningUsername, kProvisioningPassword)) {
    return config_server.requestAuthentication();
  }

  String page = R"HTML(<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{font-family:Arial;margin:24px;max-width:620px}label{display:block;margin-top:12px}input,textarea{box-sizing:border-box;padding:9px;width:100%}textarea{min-height:140px}button{margin-top:20px;padding:10px 16px}.pair{display:grid;gap:12px;grid-template-columns:1fr 1fr}</style></head><body>
<h1>Rush IoT setup</h1><form method="post" action="/save">
<label>Wi-Fi SSID<input name="wifi_ssid" value="%WIFI_SSID%" required></label>
<label>Wi-Fi password<input name="wifi_password" type="password"></label>
<label><input name="use_dhcp" type="checkbox" %DHCP_CHECKED%> Use DHCP</label>
<div class="pair"><label>Static IP<input name="static_ip" value="%STATIC_IP%"></label><label>Gateway<input name="gateway" value="%GATEWAY%"></label><label>Subnet<input name="subnet" value="%SUBNET%"></label><label>DNS<input name="dns" value="%DNS%"></label></div>
<label>MQTT host<input name="mqtt_host" value="%MQTT_HOST%" required></label>
<div class="pair"><label>MQTT TLS port<input value="8883" readonly></label><label>Telemetry interval (ms)<input name="telemetry_interval_ms" type="number" value="%INTERVAL%" required></label></div>
<label>Device token<input name="device_token" type="password" required></label>
<label>MQTT CA PEM<textarea name="mqtt_ca_pem" required></textarea></label>
<button type="submit">Save and restart</button></form></body></html>)HTML";

  page.replace("%WIFI_SSID%", html_escape(as_string(config.wifi_ssid)));
  page.replace("%DHCP_CHECKED%", config.use_dhcp ? "checked" : "");
  page.replace("%STATIC_IP%", html_escape(as_string(config.static_ip)));
  page.replace("%GATEWAY%", html_escape(as_string(config.gateway)));
  page.replace("%SUBNET%", html_escape(as_string(config.subnet)));
  page.replace("%DNS%", html_escape(as_string(config.dns)));
  page.replace("%MQTT_HOST%", html_escape(as_string(config.mqtt_host)));
  page.replace("%INTERVAL%", String(config.telemetry_interval_ms));
  config_server.send(200, "text/html", page);
}

void save_config_from_form() {
  if (!config_server.authenticate(kProvisioningUsername, kProvisioningPassword)) {
    return config_server.requestAuthentication();
  }

  iot::DeviceConfig next_config = config;
  next_config.wifi_ssid = as_std_string(config_server.arg("wifi_ssid"));
  if (config_server.arg("wifi_password").length() > 0) {
    next_config.wifi_password = as_std_string(config_server.arg("wifi_password"));
  }
  next_config.use_dhcp = config_server.hasArg("use_dhcp");
  next_config.static_ip = as_std_string(config_server.arg("static_ip"));
  next_config.gateway = as_std_string(config_server.arg("gateway"));
  next_config.subnet = as_std_string(config_server.arg("subnet"));
  next_config.dns = as_std_string(config_server.arg("dns"));
  next_config.mqtt_host = as_std_string(config_server.arg("mqtt_host"));
  next_config.mqtt_port = kMqttTlsPort;
  next_config.device_token = as_std_string(config_server.arg("device_token"));
  next_config.mqtt_ca_pem = as_std_string(config_server.arg("mqtt_ca_pem"));
  next_config.telemetry_interval_ms =
      static_cast<unsigned long>(config_server.arg("telemetry_interval_ms").toInt());

  if (!next_config.is_valid()) {
    config_server.send(400, "text/plain", "Invalid device, network, or MQTT configuration.");
    return;
  }

  save_config(next_config);
  config_server.send(200, "text/plain", "Configuration saved. Restarting device.");
  delay(500);
  ESP.restart();
}

}  // namespace

namespace {

void handle_rpc_mqtt_data(const esp_mqtt_event_handle_t event);

void start_provisioning() {
  provisioning = true;
  if (mqtt_client != nullptr && mqtt_started) {
    esp_mqtt_client_stop(mqtt_client);
    mqtt_started = false;
  }
  mqtt_connected = false;
  WiFi.disconnect(true, true);
  WiFi.mode(WIFI_AP);
  WiFi.softAP("Rush-IoT-Setup", kProvisioningPassword);
  config_server.on("/", HTTP_GET, render_config_form);
  config_server.on("/save", HTTP_POST, save_config_from_form);
  config_server.begin();
  Serial.printf("Provisioning AP ready at http://%s\n", WiFi.softAPIP().toString().c_str());
}

bool configure_static_ip() {
  IPAddress static_ip;
  IPAddress gateway;
  IPAddress subnet;
  IPAddress dns;
  if (!static_ip.fromString(as_string(config.static_ip)) ||
      !gateway.fromString(as_string(config.gateway)) ||
      !subnet.fromString(as_string(config.subnet)) || !dns.fromString(as_string(config.dns))) {
    return false;
  }

  return WiFi.config(static_ip, gateway, subnet, dns);
}

void start_station() {
  provisioning = false;
  WiFi.mode(WIFI_STA);
  if (!config.use_dhcp && !configure_static_ip()) {
    start_provisioning();
    return;
  }
  WiFi.begin(as_string(config.wifi_ssid).c_str(), as_string(config.wifi_password).c_str());
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  wifi_connect_started_at = millis();
}

bool time_is_synchronized() {
  time_t now;
  time(&now);
  return now >= kMinimumValidEpoch;
}

void configure_mqtt() {
  mqtt_client_id = "rush-iot-" + WiFi.macAddress();
  const esp_mqtt_client_config_t mqtt_config = {
      .host = config.mqtt_host.c_str(),
      .port = config.mqtt_port,
      .client_id = mqtt_client_id.c_str(),
      .username = config.device_token.c_str(),
      .password = "",
      .cert_pem = config.mqtt_ca_pem.c_str(),
      .transport = MQTT_TRANSPORT_OVER_SSL,
      .skip_cert_common_name_check = false,
  };

  mqtt_client = esp_mqtt_client_init(&mqtt_config);
  if (mqtt_client == nullptr) {
    return;
  }

  esp_mqtt_client_register_event(
      mqtt_client, MQTT_EVENT_ANY,
      [](void*, esp_event_base_t, int32_t event_id, void* event_data) {
        if (event_id == MQTT_EVENT_CONNECTED) {
          mqtt_connected = true;
          const std::string topic = iot::one_way_rpc_request_topic();
          esp_mqtt_client_subscribe(mqtt_client, topic.c_str(), iot::one_way_rpc_qos());
          const std::string gateway_topic = iot::one_way_gateway_rpc_request_topic();
          esp_mqtt_client_subscribe(mqtt_client, gateway_topic.c_str(), iot::one_way_rpc_qos());
        } else if (event_id == MQTT_EVENT_DISCONNECTED) {
          mqtt_connected = false;
          rpc_payload_active = false;
          rpc_payload = "";
          rpc_payload_total_length = 0;
          rpc_response_to_gateway = false;
        } else if (event_id == MQTT_EVENT_DATA && event_data != nullptr) {
          handle_rpc_mqtt_data(static_cast<esp_mqtt_event_handle_t>(event_data));
        }
      },
      nullptr);
}

void publish_telemetry() {
  if (!mqtt_connected) {
    return;
  }

  JsonDocument document;
  document["schema_version"] = 1;
  document["boot_id"] = boot_id;
  document["sequence"] = ++sequence;
  document["event_at"] = iso_timestamp();
  JsonObject measurements = document["measurements"].to<JsonObject>();
  measurements["adc_raw"] = analogRead(kAdcPin);
  measurements["uptime_ms"] = millis();

  String payload;
  serializeJson(document, payload);
  const std::string topic = iot::telemetry_topic();
  esp_mqtt_client_publish(mqtt_client, topic.c_str(), payload.c_str(), payload.length(),
                          iot::telemetry_qos(), 0);
}

void restart_from_one_way_rpc() {
  ESP.restart();
}

class FirmwareRpcActions final : public iot::OneWayRpcActions {
 public:
  void sample_now() override {
    persist_last_processed_rpc_id();
    publish_telemetry();
  }

  void reboot() override {
    if (persist_last_processed_rpc_id()) {
      restart_from_one_way_rpc();
    }
  }

  void publish_two_way_response(const std::string& id, const std::string& response) override {
    if (!mqtt_connected || mqtt_client == nullptr || !persist_last_processed_rpc_id()) {
      return;
    }
    const std::string topic = iot::rpc_response_topic(id, rpc_response_to_gateway);
    esp_mqtt_client_publish(mqtt_client, topic.c_str(), response.c_str(), response.size(),
                            iot::one_way_rpc_qos(), 0);
  }
};

FirmwareRpcActions rpc_actions;

void reset_rpc_payload() {
  rpc_payload_active = false;
  rpc_payload = "";
  rpc_payload_total_length = 0;
}

bool is_one_way_rpc_topic(const esp_mqtt_event_handle_t event) {
  if (event->topic == nullptr || event->topic_len < 0) {
    return false;
  }
  return iot::matches_one_way_rpc_request_topic(
      std::string(event->topic, static_cast<std::size_t>(event->topic_len)));
}

void handle_rpc_mqtt_data(const esp_mqtt_event_handle_t event) {
  if (event->data == nullptr || event->data_len < 0 || event->total_data_len <= 0 ||
      event->total_data_len > static_cast<int>(kMaximumRpcPayloadBytes)) {
    reset_rpc_payload();
    return;
  }

  if (event->current_data_offset == 0) {
    reset_rpc_payload();
    if (!is_one_way_rpc_topic(event)) {
      return;
    }
    rpc_response_to_gateway = iot::is_gateway_rpc_request_topic(
        std::string(event->topic, static_cast<std::size_t>(event->topic_len)));
    rpc_payload_active = true;
    rpc_payload_total_length = event->total_data_len;
    rpc_payload.reserve(static_cast<unsigned int>(event->total_data_len));
  }

  if (!rpc_payload_active || event->total_data_len != rpc_payload_total_length ||
      event->current_data_offset != static_cast<int>(rpc_payload.length()) ||
      event->data_len > rpc_payload_total_length - static_cast<int>(rpc_payload.length())) {
    reset_rpc_payload();
    return;
  }

  rpc_payload.concat(event->data, static_cast<unsigned int>(event->data_len));
  if (rpc_payload.length() != static_cast<unsigned int>(rpc_payload_total_length)) {
    return;
  }

  time_t now;
  time(&now);
  rpc_processor.handle(
      std::string(rpc_payload.c_str(), static_cast<std::size_t>(rpc_payload.length())), now,
      rpc_actions);
  reset_rpc_payload();
}

void ensure_mqtt_connection() {
  if (WiFi.status() != WL_CONNECTED || mqtt_client == nullptr || mqtt_started ||
      !time_is_synchronized() || millis() < mqtt_retry_at) {
    return;
  }

  mqtt_retry_at = millis() + kMqttReconnectDelayMs;
  mqtt_started = esp_mqtt_client_start(mqtt_client) == ESP_OK;
}

void maintain_station_connection() {
  if (WiFi.status() == WL_CONNECTED) {
    ensure_mqtt_connection();
    return;
  }

  if (millis() - wifi_connect_started_at >= kProvisioningFallbackMs) {
    start_provisioning();
  }
}

}  // namespace

void setup() {
  Serial.begin(115200);
  pinMode(kProvisionButtonPin, INPUT_PULLUP);
  pinMode(kAdcPin, INPUT);

  if (digitalRead(kProvisionButtonPin) == LOW) {
    clear_config();
  }

  const std::string generated_boot_id =
      iot::uuid_v4(esp_random(), esp_random(), esp_random(), esp_random());
  boot_id = as_string(generated_boot_id);
  if (!read_config()) {
    start_provisioning();
    return;
  }

  restore_last_processed_rpc_id();
  configure_mqtt();
  start_station();
}

void loop() {
  if (provisioning) {
    config_server.handleClient();
    delay(10);
    return;
  }

  maintain_station_connection();
  if (mqtt_connected && millis() >= next_publish_at) {
    publish_telemetry();
    next_publish_at = millis() + config.telemetry_interval_ms;
  }
}
