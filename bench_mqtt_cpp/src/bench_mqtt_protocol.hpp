#pragma once

#include <cstdint>
#include <cstring>
#include <string>

namespace bench_mqtt {

// 默认与 Zenoh 版本一致的 payload 大小（可被发送端参数覆盖）
static constexpr std::size_t kPayloadBytes = 1024;

// MQTT 版本默认主题（与 Zenoh key 对应）
static constexpr const char* kDefaultReqTopic = "demo/mqtt/bench/req";
static constexpr const char* kDefaultAckTopic = "demo/mqtt/bench/ack";

#pragma pack(push, 1)
struct ReqHeader {
  std::uint64_t seq;
  std::uint64_t client_send_mono_ns;
};

struct AckHeader {
  std::uint64_t seq;
  std::uint64_t server_recv_mono_ns;
  std::uint64_t server_send_mono_ns;
};
#pragma pack(pop)

static_assert(sizeof(ReqHeader) == 16, "ReqHeader size must be 16 bytes");
static_assert(sizeof(AckHeader) == 24, "AckHeader size must be 24 bytes");

inline std::string make_req_payload(std::uint64_t seq,
                                    std::uint64_t client_send_mono_ns,
                                    std::size_t payload_bytes = kPayloadBytes) {
  if (payload_bytes < sizeof(ReqHeader)) {
    payload_bytes = sizeof(ReqHeader);
  }

  std::string payload(payload_bytes, '\0');
  ReqHeader hdr{seq, client_send_mono_ns};
  std::memcpy(&payload[0], &hdr, sizeof(hdr));
  return payload;
}

inline std::string make_ack_payload(std::uint64_t seq,
                                    std::uint64_t server_recv_mono_ns,
                                    std::uint64_t server_send_mono_ns) {
  std::string payload(sizeof(AckHeader), '\0');
  AckHeader hdr{seq, server_recv_mono_ns, server_send_mono_ns};
  std::memcpy(&payload[0], &hdr, sizeof(hdr));
  return payload;
}

inline bool parse_req_payload(const void* data, std::size_t len, ReqHeader& out) {
  if (len < sizeof(ReqHeader)) return false;
  std::memcpy(&out, data, sizeof(ReqHeader));
  return true;
}

inline bool parse_ack_payload(const void* data, std::size_t len, AckHeader& out) {
  if (len < sizeof(AckHeader)) return false;
  std::memcpy(&out, data, sizeof(AckHeader));
  return true;
}

}  // namespace bench_mqtt

