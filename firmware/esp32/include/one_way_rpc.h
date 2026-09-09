#pragma once

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <string>

namespace iot {

inline std::string one_way_rpc_request_topic() {
  return "v1/devices/me/rpc/request/+";
}

inline std::string one_way_gateway_rpc_request_topic() {
  return "v1/gateways/me/rpc/request/+";
}

inline std::string rpc_response_topic(const std::string& request_id, const bool gateway) {
  return std::string(gateway ? "v1/gateways/me/rpc/response/" : "v1/devices/me/rpc/response/") +
         request_id;
}

inline bool is_gateway_rpc_request_topic(const std::string& topic) {
  static constexpr const char* kGatewayPrefix = "v1/gateways/me/rpc/request/";
  const std::string prefix(kGatewayPrefix);
  return topic.size() > prefix.size() && topic.compare(0, prefix.size(), prefix) == 0;
}

inline bool matches_one_way_rpc_request_topic(const std::string& topic) {
  static constexpr const char* kDirectPrefix = "v1/devices/me/rpc/request/";
  static constexpr const char* kGatewayPrefix = "v1/gateways/me/rpc/request/";
  const auto matches = [&topic](const char* prefix) {
    const std::string prefix_value(prefix);
    const std::string::size_type request_offset = prefix_value.size();
    if (topic.size() <= request_offset || topic.compare(0, request_offset, prefix_value) != 0) {
      return false;
    }
    const std::string request_id = topic.substr(request_offset);
    return request_id != "+" && request_id != "#" && request_id.find('/') == std::string::npos;
  };
  return matches(kDirectPrefix) || matches(kGatewayPrefix);
}

inline constexpr int one_way_rpc_qos() {
  return 1;
}

enum class OneWayRpcResult {
  Executed,
  RejectedTwoWay,
  IgnoredMalformed,
  IgnoredExpired,
  IgnoredDuplicate,
  IgnoredUnsupported,
};

class OneWayRpcActions {
 public:
  virtual ~OneWayRpcActions() = default;
  virtual void sample_now() = 0;
  virtual void reboot() = 0;
  virtual void publish_two_way_response(const std::string&, const std::string&) {}
};

namespace detail {

class JsonCommandReader {
 public:
  explicit JsonCommandReader(const std::string& input) : input_(input) {}

  bool read(std::string& id, std::string& method, std::string& issued_at,
            std::string& expires_at, std::string& mode) {
    bool has_id = false;
    bool has_method = false;
    bool has_params = false;
    bool has_issued_at = false;
    bool has_expires_at = false;
    bool has_mode = false;
    mode = "one_way";

    whitespace();
    if (!consume('{')) {
      return false;
    }
    whitespace();
    if (consume('}')) {
      return false;
    }

    while (true) {
      std::string key;
      if (!string(key)) {
        return false;
      }
      whitespace();
      if (!consume(':')) {
        return false;
      }
      whitespace();

      if (key == "id") {
        if (has_id || !string(id)) {
          return false;
        }
        has_id = true;
      } else if (key == "method") {
        if (has_method || !string(method)) {
          return false;
        }
        has_method = true;
      } else if (key == "params") {
        if (has_params || !object(1)) {
          return false;
        }
        has_params = true;
      } else if (key == "issued_at") {
        if (has_issued_at || !string(issued_at)) {
          return false;
        }
        has_issued_at = true;
      } else if (key == "expires_at") {
        if (has_expires_at || !string(expires_at)) {
          return false;
        }
        has_expires_at = true;
      } else if (key == "mode") {
        if (has_mode || !string(mode)) {
          return false;
        }
        has_mode = true;
      } else if (!value(1)) {
        return false;
      }

      whitespace();
      if (consume('}')) {
        break;
      }
      if (!consume(',')) {
        return false;
      }
      whitespace();
    }

    whitespace();
    return position_ == input_.size() && has_id && has_method && has_params && has_issued_at &&
           has_expires_at;
  }

 private:
  static constexpr unsigned int kMaximumDepth = 16;

  void whitespace() {
    while (position_ < input_.size()) {
      const char character = input_[position_];
      if (character != ' ' && character != '\t' && character != '\r' && character != '\n') {
        return;
      }
      ++position_;
    }
  }

  bool consume(const char expected) {
    if (position_ == input_.size() || input_[position_] != expected) {
      return false;
    }
    ++position_;
    return true;
  }

  bool string(std::string& output) {
    if (!consume('"')) {
      return false;
    }
    output.clear();
    while (position_ < input_.size()) {
      const unsigned char character = static_cast<unsigned char>(input_[position_++]);
      if (character == '"') {
        return true;
      }
      if (character < 0x20) {
        return false;
      }
      if (character != '\\') {
        output.push_back(static_cast<char>(character));
        continue;
      }
      if (position_ == input_.size()) {
        return false;
      }

      const char escaped = input_[position_++];
      if (escaped == '"' || escaped == '\\' || escaped == '/') {
        output.push_back(escaped);
      } else if (escaped == 'b') {
        output.push_back('\b');
      } else if (escaped == 'f') {
        output.push_back('\f');
      } else if (escaped == 'n') {
        output.push_back('\n');
      } else if (escaped == 'r') {
        output.push_back('\r');
      } else if (escaped == 't') {
        output.push_back('\t');
      } else if (escaped == 'u') {
        if (!unicode_escape(output)) {
          return false;
        }
      } else {
        return false;
      }
    }
    return false;
  }

  bool unicode_escape(std::string& output) {
    if (position_ + 4 > input_.size()) {
      return false;
    }
    unsigned int value = 0;
    for (unsigned int index = 0; index < 4; ++index) {
      const char character = input_[position_++];
      if (!std::isxdigit(static_cast<unsigned char>(character))) {
        return false;
      }
      value = value * 16 + (character >= '0' && character <= '9'
                                ? static_cast<unsigned int>(character - '0')
                                : character >= 'a' && character <= 'f'
                                      ? static_cast<unsigned int>(character - 'a' + 10)
                                      : static_cast<unsigned int>(character - 'A' + 10));
    }
    output.push_back(value <= 0x7f ? static_cast<char>(value) : '?');
    return true;
  }

  bool object(const unsigned int depth) {
    if (depth > kMaximumDepth || !consume('{')) {
      return false;
    }
    whitespace();
    if (consume('}')) {
      return true;
    }
    while (true) {
      std::string ignored;
      if (!string(ignored)) {
        return false;
      }
      whitespace();
      if (!consume(':')) {
        return false;
      }
      if (!value(depth + 1)) {
        return false;
      }
      whitespace();
      if (consume('}')) {
        return true;
      }
      if (!consume(',')) {
        return false;
      }
      whitespace();
    }
  }

  bool array(const unsigned int depth) {
    if (depth > kMaximumDepth || !consume('[')) {
      return false;
    }
    whitespace();
    if (consume(']')) {
      return true;
    }
    while (true) {
      if (!value(depth + 1)) {
        return false;
      }
      whitespace();
      if (consume(']')) {
        return true;
      }
      if (!consume(',')) {
        return false;
      }
      whitespace();
    }
  }

  bool number() {
    consume('-');
    if (consume('0')) {
      if (position_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[position_]))) {
        return false;
      }
    } else {
      const std::size_t integer_start = position_;
      while (position_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[position_]))) {
        ++position_;
      }
      if (integer_start == position_) {
        return false;
      }
    }
    if (consume('.')) {
      const std::size_t fraction_start = position_;
      while (position_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[position_]))) {
        ++position_;
      }
      if (fraction_start == position_) {
        return false;
      }
    }
    if (position_ < input_.size() && (input_[position_] == 'e' || input_[position_] == 'E')) {
      ++position_;
      if (position_ < input_.size() && (input_[position_] == '+' || input_[position_] == '-')) {
        ++position_;
      }
      const std::size_t exponent_start = position_;
      while (position_ < input_.size() && std::isdigit(static_cast<unsigned char>(input_[position_]))) {
        ++position_;
      }
      if (exponent_start == position_) {
        return false;
      }
    }
    return true;
  }

  bool literal(const char* expected) {
    while (*expected != '\0') {
      if (position_ == input_.size() || input_[position_++] != *expected++) {
        return false;
      }
    }
    return true;
  }

  bool value(const unsigned int depth) {
    if (depth > kMaximumDepth) {
      return false;
    }
    whitespace();
    if (position_ == input_.size()) {
      return false;
    }
    const char character = input_[position_];
    if (character == '"') {
      std::string ignored;
      return string(ignored);
    }
    if (character == '{') {
      return object(depth);
    }
    if (character == '[') {
      return array(depth);
    }
    if (character == 't') {
      return literal("true");
    }
    if (character == 'f') {
      return literal("false");
    }
    if (character == 'n') {
      return literal("null");
    }
    if (character == '-' || std::isdigit(static_cast<unsigned char>(character))) {
      return number();
    }
    return false;
  }

  const std::string& input_;
  std::size_t position_ = 0;
};

}  // namespace detail

class OneWayRpcProcessor {
 public:
  OneWayRpcResult handle(const std::string& payload, const std::time_t now,
                         OneWayRpcActions& actions) {
    std::string id;
    std::string method;
    std::string issued_at;
    std::string expires_at;
    std::string mode;
    if (!detail::JsonCommandReader(payload).read(id, method, issued_at, expires_at, mode) ||
        !is_uuid_v7(id) || !is_method(method) || (mode != "one_way" && mode != "two_way")) {
      return OneWayRpcResult::IgnoredMalformed;
    }

    std::int64_t issued_at_ms = 0;
    std::int64_t expires_at_ms = 0;
    if (!timestamp_ms(issued_at, issued_at_ms) || !timestamp_ms(expires_at, expires_at_ms) ||
        issued_at_ms >= expires_at_ms) {
      return OneWayRpcResult::IgnoredMalformed;
    }
    if (expires_at_ms <= static_cast<std::int64_t>(now) * 1000) {
      return OneWayRpcResult::IgnoredExpired;
    }
    if (contains(id)) {
      return OneWayRpcResult::IgnoredDuplicate;
    }
    if (method != "sample_now" && method != "reboot") {
      if (mode == "two_way") {
        remember(id);
        actions.publish_two_way_response(id, "{\"ok\":false,\"error\":\"unsupported_method\"}");
      }
      return OneWayRpcResult::IgnoredUnsupported;
    }

    remember(id);
    if (method == "sample_now") {
      actions.sample_now();
      if (mode == "two_way") {
        actions.publish_two_way_response(id, "{\"ok\":true,\"result\":{\"sampled\":true}}");
      }
      return OneWayRpcResult::Executed;
    }
    if (mode == "two_way") {
      actions.publish_two_way_response(id, "{\"ok\":false,\"error\":\"two_way_reboot_unsupported\"}");
      return OneWayRpcResult::RejectedTwoWay;
    }
    actions.reboot();
    return OneWayRpcResult::Executed;
  }

  void restore_processed_id(const std::string& id) {
    if (is_uuid_v7(id) && !contains(id)) {
      remember(id);
    }
  }

  std::string latest_processed_id() const {
    if (processed_count_ == 0) {
      return "";
    }
    const std::size_t index = (next_processed_index_ + processed_ids_.size() - 1) %
                              processed_ids_.size();
    return processed_ids_[index];
  }

 private:
  static bool is_uuid_v7(const std::string& id) {
    if (id.size() != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' ||
        id[14] != '7' || (id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b' &&
                          id[19] != 'A' && id[19] != 'B')) {
      return false;
    }
    for (std::size_t index = 0; index < id.size(); ++index) {
      if (index == 8 || index == 13 || index == 18 || index == 23) {
        continue;
      }
      if (!std::isxdigit(static_cast<unsigned char>(id[index]))) {
        return false;
      }
    }
    return true;
  }

  static bool is_method(const std::string& method) {
    if (method.empty() || method.size() > 64) {
      return false;
    }
    for (std::size_t index = 0; index < method.size(); ++index) {
      const char character = method[index];
      if (!((character >= 'A' && character <= 'Z') ||
            (character >= 'a' && character <= 'z') ||
            (character >= '0' && character <= '9') || character == '_' || character == '-')) {
        return false;
      }
    }
    return true;
  }

  static bool fixed_decimal(const std::string& input, const std::size_t offset,
                            const std::size_t width, int& value) {
    if (offset + width > input.size()) {
      return false;
    }
    value = 0;
    for (std::size_t index = offset; index < offset + width; ++index) {
      if (!std::isdigit(static_cast<unsigned char>(input[index]))) {
        return false;
      }
      value = value * 10 + (input[index] - '0');
    }
    return true;
  }

  static bool is_leap_year(const int year) {
    return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
  }

  static bool timestamp_ms(const std::string& value, std::int64_t& milliseconds) {
    if (value.size() < 20 || value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
        value[13] != ':' || value[16] != ':') {
      return false;
    }

    int year = 0;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!fixed_decimal(value, 0, 4, year) || !fixed_decimal(value, 5, 2, month) ||
        !fixed_decimal(value, 8, 2, day) || !fixed_decimal(value, 11, 2, hour) ||
        !fixed_decimal(value, 14, 2, minute) || !fixed_decimal(value, 17, 2, second) ||
        month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59) {
      return false;
    }

    static const int days_in_month[] = {31, 28, 31, 30, 31, 30,
                                        31, 31, 30, 31, 30, 31};
    const int maximum_day = days_in_month[month - 1] + (month == 2 && is_leap_year(year) ? 1 : 0);
    if (day < 1 || day > maximum_day) {
      return false;
    }

    std::size_t position = 19;
    int fraction_ms = 0;
    if (position < value.size() && value[position] == '.') {
      ++position;
      const std::size_t fraction_start = position;
      unsigned int retained_digits = 0;
      while (position < value.size() &&
             std::isdigit(static_cast<unsigned char>(value[position]))) {
        if (retained_digits < 3) {
          fraction_ms = fraction_ms * 10 + (value[position] - '0');
          ++retained_digits;
        }
        ++position;
      }
      if (position == fraction_start) {
        return false;
      }
      while (retained_digits++ < 3) {
        fraction_ms *= 10;
      }
    }
    if (position + 1 != value.size() || value[position] != 'Z') {
      return false;
    }

    const int adjusted_year = year - (month <= 2 ? 1 : 0);
    const int era = (adjusted_year >= 0 ? adjusted_year : adjusted_year - 399) / 400;
    const unsigned int year_of_era = static_cast<unsigned int>(adjusted_year - era * 400);
    const unsigned int day_of_year =
        (153 * static_cast<unsigned int>(month + (month > 2 ? -3 : 9)) + 2) / 5 +
        static_cast<unsigned int>(day - 1);
    const unsigned int day_of_era =
        year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    const std::int64_t days =
        static_cast<std::int64_t>(era) * 146097 + static_cast<std::int64_t>(day_of_era) - 719468;
    milliseconds =
        (days * 86400 + hour * 3600 + minute * 60 + second) * 1000 + fraction_ms;
    return true;
  }

  bool contains(const std::string& id) const {
    for (std::size_t index = 0; index < processed_count_; ++index) {
      if (processed_ids_[index] == id) {
        return true;
      }
    }
    return false;
  }

  void remember(const std::string& id) {
    processed_ids_[next_processed_index_] = id;
    next_processed_index_ = (next_processed_index_ + 1) % processed_ids_.size();
    if (processed_count_ < processed_ids_.size()) {
      ++processed_count_;
    }
  }

  std::array<std::string, 16> processed_ids_;
  std::size_t next_processed_index_ = 0;
  std::size_t processed_count_ = 0;
};

}  // namespace iot
