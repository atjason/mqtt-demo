#include "bench_mqtt_protocol.hpp"

#include <mosquitto.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

struct OnlineStats {
  std::uint64_t n = 0;
  double mean = 0.0;
  double m2 = 0.0;
  double min_v = std::numeric_limits<double>::infinity();
  double max_v = -std::numeric_limits<double>::infinity();

  void add(double x) {
    ++n;
    if (x < min_v) min_v = x;
    if (x > max_v) max_v = x;
    const double delta = x - mean;
    mean += delta / static_cast<double>(n);
    const double delta2 = x - mean;
    m2 += delta * delta2;
  }

  double variance() const { return (n >= 2) ? (m2 / static_cast<double>(n - 1)) : 0.0; }
  double stddev() const { return std::sqrt(variance()); }
};

struct Args {
  std::string host = "127.0.0.1";
  int port = 1883;
  std::string req_topic = bench_mqtt::kDefaultReqTopic;
  std::string ack_topic = bench_mqtt::kDefaultAckTopic;
  int rate_hz = 1000;
  std::size_t payload_bytes = bench_mqtt::kPayloadBytes;
  std::uint64_t count = 0;
  double duration_sec = 10.0;
  int ack_timeout_ms = 100;
  int qos = 1;
  bool quiet = false;
};

bool parse_args(int argc, char** argv, Args& out) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto need = [&](const char* name) -> const char* {
      if (i + 1 >= argc) {
        std::cerr << "Missing value for " << name << "\n";
        return nullptr;
      }
      return argv[++i];
    };

    if (a == "--host") {
      const char* v = need("--host");
      if (!v) return false;
      out.host = v;
    } else if (a == "--port") {
      const char* v = need("--port");
      if (!v) return false;
      out.port = std::atoi(v);
    } else if (a == "--req-topic") {
      const char* v = need("--req-topic");
      if (!v) return false;
      out.req_topic = v;
    } else if (a == "--ack-topic") {
      const char* v = need("--ack-topic");
      if (!v) return false;
      out.ack_topic = v;
    } else if (a == "--rate-hz") {
      const char* v = need("--rate-hz");
      if (!v) return false;
      out.rate_hz = std::atoi(v);
    } else if (a == "--payload-bytes") {
      const char* v = need("--payload-bytes");
      if (!v) return false;
      out.payload_bytes = static_cast<std::size_t>(std::strtoull(v, nullptr, 10));
    } else if (a == "--count") {
      const char* v = need("--count");
      if (!v) return false;
      out.count = static_cast<std::uint64_t>(std::strtoull(v, nullptr, 10));
    } else if (a == "--duration-sec") {
      const char* v = need("--duration-sec");
      if (!v) return false;
      out.duration_sec = std::atof(v);
    } else if (a == "--ack-timeout-ms") {
      const char* v = need("--ack-timeout-ms");
      if (!v) return false;
      out.ack_timeout_ms = std::atoi(v);
    } else if (a == "--qos") {
      const char* v = need("--qos");
      if (!v) return false;
      out.qos = std::atoi(v);
    } else if (a == "--quiet") {
      out.quiet = true;
    } else if (a == "-h" || a == "--help") {
      std::cout
          << "bench_mqtt_pub_rtt\n\n"
          << "  --host           <string>  (default: 127.0.0.1)\n"
          << "  --port           <int>     (default: 1883)\n"
          << "  --req-topic      <string>  (default: " << bench_mqtt::kDefaultReqTopic << ")\n"
          << "  --ack-topic      <string>  (default: " << bench_mqtt::kDefaultAckTopic << ")\n"
          << "  --rate-hz        <int>     (default: 1000)\n"
          << "  --payload-bytes  <int>     (default: " << bench_mqtt::kPayloadBytes
          << ", must be >= " << sizeof(bench_mqtt::ReqHeader) << ")\n"
          << "  --count          <uint64>  (if set, ignore --duration-sec)\n"
          << "  --duration-sec   <double>  (default: 10.0)\n"
          << "  --ack-timeout-ms <int>     (default: 100)\n"
          << "  --qos            <0|1|2>   (default: 1)\n"
          << "  --quiet                    (reduce logs)\n";
      std::exit(0);
    } else {
      std::cerr << "Unknown arg: " << a << "\n";
      return false;
    }
  }

  if (out.port <= 0 || out.port > 65535) {
    std::cerr << "--port must be in (0, 65535]\n";
    return false;
  }
  if (out.rate_hz <= 0) {
    std::cerr << "--rate-hz must be > 0\n";
    return false;
  }
  if (out.payload_bytes < sizeof(bench_mqtt::ReqHeader)) {
    std::cerr << "--payload-bytes must be >= " << sizeof(bench_mqtt::ReqHeader) << "\n";
    return false;
  }
  if (out.qos < 0 || out.qos > 2) {
    std::cerr << "--qos must be 0, 1 or 2\n";
    return false;
  }
  return true;
}

template <class T>
double percentile_sorted(const std::vector<T>& sorted, double p01) {
  if (sorted.empty()) return 0.0;
  if (p01 <= 0.0) return static_cast<double>(sorted.front());
  if (p01 >= 1.0) return static_cast<double>(sorted.back());
  const double idx = p01 * static_cast<double>(sorted.size() - 1);
  const std::size_t i = static_cast<std::size_t>(idx);
  return static_cast<double>(sorted[i]);
}

std::uint64_t steady_now_ns() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running.store(false); }

using Clock = std::chrono::steady_clock;
using TP = Clock::time_point;

struct SharedContext {
  std::mutex mu;
  std::vector<TP> send_ts;
  std::vector<std::uint8_t> state;  // 0=unsent,1=inflight,2=acked,3=timedout
  std::deque<std::uint64_t> inflight;
  std::unordered_map<std::uint64_t, TP> send_map;
  std::vector<double> rtt_us_samples;
  OnlineStats rtt_us_stats;
  std::uint64_t ack_received = 0;
  std::uint64_t timeouts = 0;
  std::uint64_t out_of_order = 0;
  std::uint64_t last_ack_seq = 0;
  bool have_last_ack_seq = false;
  Args* args = nullptr;
};

void on_message(struct mosquitto* /*mosq*/, void* userdata,
                const struct mosquitto_message* msg) {
  auto* ctx = static_cast<SharedContext*>(userdata);
  if (!ctx || !ctx->args) return;

  bench_mqtt::AckHeader ack{};
  if (!bench_mqtt::parse_ack_payload(msg->payload,
                                     static_cast<std::size_t>(msg->payloadlen),
                                     ack)) {
    return;
  }

  const auto now_tp = Clock::now();

  std::lock_guard<std::mutex> lk(ctx->mu);

  if (ctx->have_last_ack_seq && ack.seq <= ctx->last_ack_seq) ++ctx->out_of_order;
  ctx->last_ack_seq = ack.seq;
  ctx->have_last_ack_seq = true;

  if (ctx->args->count > 0) {
    if (ack.seq >= ctx->args->count) return;
    const std::size_t idx = static_cast<std::size_t>(ack.seq);
    if (idx >= ctx->state.size()) return;
    if (ctx->state[idx] != 1) return;
    const auto sent_tp = ctx->send_ts[idx];
    const auto rtt =
        std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(now_tp -
                                                                              sent_tp);
    ctx->state[idx] = 2;
    ++ctx->ack_received;
    ctx->rtt_us_stats.add(rtt.count());
    ctx->rtt_us_samples.push_back(rtt.count());
  } else {
    auto it = ctx->send_map.find(ack.seq);
    if (it == ctx->send_map.end()) return;
    const auto rtt =
        std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(now_tp -
                                                                              it->second);
    ctx->send_map.erase(it);
    ++ctx->ack_received;
    ctx->rtt_us_stats.add(rtt.count());
    ctx->rtt_us_samples.push_back(rtt.count());
  }
}

void on_connect(struct mosquitto* mosq, void* userdata, int rc) {
  auto* ctx = static_cast<SharedContext*>(userdata);
  if (!ctx || !ctx->args) return;
  if (rc != 0) {
    std::cerr << "MQTT connect failed: " << rc << "\n";
    return;
  }
  int sub_rc =
      mosquitto_subscribe(mosq, nullptr, ctx->args->ack_topic.c_str(), ctx->args->qos);
  if (sub_rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "Failed to subscribe to " << ctx->args->ack_topic
              << ": " << mosquitto_strerror(sub_rc) << "\n";
  } else if (!ctx->args->quiet) {
    std::cout << "Subscribed to ack-topic=" << ctx->args->ack_topic
              << " qos=" << ctx->args->qos << "\n";
  }
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  if (!parse_args(argc, argv, args)) return 2;

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  int rc = mosquitto_lib_init();
  if (rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "mosquitto_lib_init failed: " << mosquitto_strerror(rc) << "\n";
    return 1;
  }

  SharedContext ctx;
  ctx.args = &args;

  struct mosquitto* mosq =
      mosquitto_new(nullptr, true /* clean_session */, &ctx);
  if (!mosq) {
    std::cerr << "mosquitto_new failed\n";
    mosquitto_lib_cleanup();
    return 1;
  }

  mosquitto_user_data_set(mosq, &ctx);
  mosquitto_connect_callback_set(mosq, on_connect);
  mosquitto_message_callback_set(mosq, on_message);

  rc = mosquitto_connect(mosq, args.host.c_str(), args.port, 60 /* keepalive */);
  if (rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "mosquitto_connect failed: " << mosquitto_strerror(rc) << "\n";
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 1;
  }

  rc = mosquitto_loop_start(mosq);
  if (rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "mosquitto_loop_start failed: " << mosquitto_strerror(rc) << "\n";
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 1;
  }

  std::cout << "bench_mqtt_pub_rtt host=" << args.host << " port=" << args.port
            << " req_topic=" << args.req_topic << " ack_topic=" << args.ack_topic
            << " rate_hz=" << args.rate_hz
            << " payload_bytes=" << args.payload_bytes
            << ((args.count > 0) ? (" count=" + std::to_string(args.count))
                                 : (" duration_sec=" + std::to_string(args.duration_sec)))
            << " ack_timeout_ms=" << args.ack_timeout_ms
            << " qos=" << args.qos << "\n";

  // Preallocate state based on mode.
  if (args.count > 0) {
    ctx.send_ts.resize(static_cast<std::size_t>(args.count));
    ctx.state.resize(static_cast<std::size_t>(args.count), 0);
    ctx.rtt_us_samples.reserve(static_cast<std::size_t>(args.count));
  } else {
    ctx.rtt_us_samples.reserve(
        static_cast<std::size_t>(args.rate_hz * args.duration_sec * 1.2));
    ctx.send_map.reserve(static_cast<std::size_t>(args.rate_hz * 2));
  }

  const auto start_tp = Clock::now();
  const auto interval =
      std::chrono::microseconds(static_cast<int>(1000000 / args.rate_hz));
  auto next_send = start_tp;

  std::uint64_t sent = 0;

  auto should_continue = [&]() -> bool {
    if (!g_running.load()) return false;
    if (args.count > 0) return sent < args.count;
    const auto now = Clock::now();
    const double elapsed =
        std::chrono::duration_cast<std::chrono::duration<double>>(now - start_tp)
            .count();
    return elapsed < args.duration_sec;
  };

  const auto timeout = std::chrono::milliseconds(args.ack_timeout_ms);

  while (should_continue()) {
    const auto now_tp = Clock::now();
    if (now_tp < next_send) {
      std::this_thread::sleep_until(next_send);
    }

    const auto send_tp = Clock::now();
    const std::uint64_t send_ns = steady_now_ns();
    const std::uint64_t seq = sent++;

    {
      std::lock_guard<std::mutex> lk(ctx.mu);
      if (args.count > 0) {
        if (seq < args.count) {
          const std::size_t idx = static_cast<std::size_t>(seq);
          ctx.send_ts[idx] = send_tp;
          ctx.state[idx] = 1;
          ctx.inflight.push_back(seq);
        }
      } else {
        ctx.send_map.emplace(seq, send_tp);
        ctx.inflight.push_back(seq);
      }
    }

    std::string payload =
        bench_mqtt::make_req_payload(seq, send_ns, args.payload_bytes);
    int pub_rc = mosquitto_publish(mosq, nullptr,
                                   args.req_topic.c_str(),
                                   static_cast<int>(payload.size()),
                                   payload.data(),
                                   args.qos,
                                   false);
    if (pub_rc != MOSQ_ERR_SUCCESS && !args.quiet) {
      std::cerr << "Failed to publish req: " << mosquitto_strerror(pub_rc) << "\n";
    }

    if (!args.quiet && (seq % 1000 == 0)) {
      std::size_t inflight_sz = 0;
      {
        std::lock_guard<std::mutex> lk(ctx.mu);
        inflight_sz = ctx.inflight.size();
      }
      std::cout << "sent seq=" << seq << " inflight=" << inflight_sz << "\n";
    }

    if (args.ack_timeout_ms > 0) {
      std::lock_guard<std::mutex> lk(ctx.mu);
      const auto now2 = Clock::now();
      while (!ctx.inflight.empty()) {
        const std::uint64_t s = ctx.inflight.front();
        if (args.count > 0) {
          const std::size_t idx = static_cast<std::size_t>(s);
          if (idx >= ctx.state.size()) {
            ctx.inflight.pop_front();
            continue;
          }
          const auto st = ctx.state[idx];
          if (st == 2 || st == 3) {
            ctx.inflight.pop_front();
            continue;
          }
          const auto age = now2 - ctx.send_ts[idx];
          if (age > timeout) {
            ctx.state[idx] = 3;
            ++ctx.timeouts;
            ctx.inflight.pop_front();
            continue;
          }
          break;
        } else {
          auto it = ctx.send_map.find(s);
          if (it == ctx.send_map.end()) {
            ctx.inflight.pop_front();
            continue;
          }
          const auto age = now2 - it->second;
          if (age > timeout) {
            ctx.send_map.erase(it);
            ++ctx.timeouts;
            ctx.inflight.pop_front();
            continue;
          }
          break;
        }
      }
    }

    next_send += interval;
  }

  if (args.ack_timeout_ms > 0) {
    const auto drain_until = Clock::now() + timeout;
    while (Clock::now() < drain_until) {
      {
        std::lock_guard<std::mutex> lk(ctx.mu);
        const auto now_tp = Clock::now();
        while (!ctx.inflight.empty()) {
          const std::uint64_t s = ctx.inflight.front();
          if (args.count > 0) {
            const std::size_t idx = static_cast<std::size_t>(s);
            if (idx >= ctx.state.size()) {
              ctx.inflight.pop_front();
              continue;
            }
            const auto st = ctx.state[idx];
            if (st == 2 || st == 3) {
              ctx.inflight.pop_front();
              continue;
            }
            const auto age = now_tp - ctx.send_ts[idx];
            if (age > timeout) {
              ctx.state[idx] = 3;
              ++ctx.timeouts;
              ctx.inflight.pop_front();
              continue;
            }
            break;
          } else {
            auto it = ctx.send_map.find(s);
            if (it == ctx.send_map.end()) {
              ctx.inflight.pop_front();
              continue;
            }
            const auto age = now_tp - it->second;
            if (age > timeout) {
              ctx.send_map.erase(it);
              ++ctx.timeouts;
              ctx.inflight.pop_front();
              continue;
            }
            break;
          }
        }
        if (ctx.inflight.empty()) break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
  }

  const auto end_tp = Clock::now();
  const double dur_s =
      std::chrono::duration_cast<std::chrono::duration<double>>(end_tp - start_tp)
          .count();
  const double sent_per_s =
      (dur_s > 0.0) ? (static_cast<double>(sent) / dur_s) : 0.0;
  const double ack_per_s =
      (dur_s > 0.0) ? (static_cast<double>(ctx.ack_received) / dur_s) : 0.0;
  const double mb_per_s =
      (dur_s > 0.0)
          ? ((static_cast<double>(sent) * args.payload_bytes) / dur_s / 1024.0 /
             1024.0)
          : 0.0;
  const double timeout_ratio_sent =
      (sent > 0)
          ? (static_cast<double>(ctx.timeouts) / static_cast<double>(sent) * 100.0)
          : 0.0;
  const double out_of_order_ratio =
      (ctx.ack_received > 0)
          ? (static_cast<double>(ctx.out_of_order) /
             static_cast<double>(ctx.ack_received) * 100.0)
          : 0.0;

  std::uint64_t pending_inflight = 0;
  {
    std::lock_guard<std::mutex> lk(ctx.mu);
    if (args.count > 0) {
      for (std::size_t i = 0; i < ctx.state.size(); ++i) {
        if (ctx.state[i] == 1) ++pending_inflight;
      }
    } else {
      pending_inflight = static_cast<std::uint64_t>(ctx.send_map.size());
    }
  }

  std::vector<double> sorted = ctx.rtt_us_samples;
  std::sort(sorted.begin(), sorted.end());

  const double p50 = percentile_sorted(sorted, 0.50);
  const double p95 = percentile_sorted(sorted, 0.95);
  const double p99 = percentile_sorted(sorted, 0.99);

  mosquitto_loop_stop(mosq, true);
  mosquitto_disconnect(mosq);
  mosquitto_destroy(mosq);
  mosquitto_lib_cleanup();

  const auto old_flags = std::cout.flags();
  const auto old_prec = std::cout.precision();
  std::cout.setf(std::ios::fixed);
  std::cout << std::setprecision(3);

  std::cout << "=== 汇总（MQTT RTT 往返时延测试）===\n"
            << "运行时长: " << dur_s << " 秒\n"
            << "发送请求: " << sent << " 条\n"
            << "收到 ACK: " << ctx.ack_received << " 条\n"
            << "超时次数: " << ctx.timeouts << " 条（占已发送 " << timeout_ratio_sent << " %）\n"
            << "乱序 ACK: " << ctx.out_of_order << " 条（占已收到 ACK " << out_of_order_ratio
            << " %）\n"
            << "在途未完成: " << pending_inflight << " 条\n"
            << "发送速率: " << sent_per_s << " 条/秒\n"
            << "ACK 速率: " << ack_per_s << " 条/秒\n"
            << "吞吐量: " << mb_per_s << " MiB/秒（payload=" << args.payload_bytes
            << " 字节）\n";

  if (ctx.rtt_us_stats.n > 0) {
    std::cout << "RTT（微秒 us）: 平均 " << ctx.rtt_us_stats.mean
              << "，最小 " << ctx.rtt_us_stats.min_v
              << "，最大 " << ctx.rtt_us_stats.max_v
              << "（约 " << (ctx.rtt_us_stats.max_v / 1000.0) << " ms）\n"
              << "RTT 分位数（微秒 us）: P50 " << p50 << "，P95 " << p95 << "，P99 " << p99
              << "\n"
              << "RTT 抖动（标准差，微秒 us）: " << ctx.rtt_us_stats.stddev() << "\n";
  } else {
    std::cout << "RTT: 无有效样本\n";
  }

  std::cout.flags(old_flags);
  std::cout.precision(old_prec);

  return 0;
}

