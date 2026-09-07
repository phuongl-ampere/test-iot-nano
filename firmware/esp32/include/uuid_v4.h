#pragma once

#include <cstdint>
#include <string>

namespace iot {

inline std::string uuid_v4(std::uint32_t first, std::uint32_t second, std::uint32_t third,
                           std::uint32_t fourth) {
  const std::uint32_t words[] = {first, second, third, fourth};
  std::uint8_t bytes[16];

  for (std::size_t word_index = 0; word_index < 4; ++word_index) {
    for (std::size_t byte_index = 0; byte_index < 4; ++byte_index) {
      const std::size_t index = word_index * 4 + byte_index;
      bytes[index] = static_cast<std::uint8_t>(words[word_index] >> ((3 - byte_index) * 8));
    }
  }

  bytes[6] = static_cast<std::uint8_t>((bytes[6] & 0x0f) | 0x40);
  bytes[8] = static_cast<std::uint8_t>((bytes[8] & 0x3f) | 0x80);

  constexpr char kHexDigits[] = "0123456789abcdef";
  std::string uuid;
  uuid.reserve(36);
  for (std::size_t index = 0; index < 16; ++index) {
    if (index == 4 || index == 6 || index == 8 || index == 10) {
      uuid.push_back('-');
    }
    uuid.push_back(kHexDigits[bytes[index] >> 4]);
    uuid.push_back(kHexDigits[bytes[index] & 0x0f]);
  }

  return uuid;
}

}  // namespace iot
