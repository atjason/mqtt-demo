#include "bench_mqtt_protocol.hpp"

#include <mosquitto.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>

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
    } else if (a == "--qos") {
      const char* v = need("--qos");
      if (!v) return false;
      out.qos = std::atoi(v);
    } else if (a == "--quiet") {
      out.quiet = true;
    } else if (a == "-h" || a == "--help") {
      std::cout
          << "bench_mqtt_echo_ack\n\n"
          << "  --host       <string>  (default: 127.0.0.1)\n"
          << "  --port       <int>     (default: 1883)\n"
          << "  --req-topic  <string>  (default: " << bench_mqtt::kDefaultReqTopic << ")\n"
          << "  --ack-topic  <string>  (default: " << bench_mqtt::kDefaultAckTopic << ")\n"
          << "  --qos        <0|1|2>   (default: 1)\n"
          << "  --quiet                (disable per-1000 message logs)\n";
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
  if (out.qos < 0 || out.qos > 2) {
    std::cerr << "--qos must be 0, 1 or 2\n";
    return false;
  }
  return true;
}

std::uint64_t steady_now_ns() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::atomic<bool> g_running{true};
void handle_signal(int) { g_running.store(false); }

struct EchoContext {
  Args args;
  using Clock = std::chrono::steady_clock;
  bool have_prev = false;
  Clock::time_point prev_tp{};
  OnlineStats interarrival_us{};
  std::uint64_t recv_count = 0;
  std::uint64_t out_of_order = 0;
  std::uint64_t last_seq = 0;
  bool have_last_seq = false;
  std::size_t last_payload_bytes = 0;
  Clock::time_point start_tp{};
  std::mutex mu;
};

void on_message(struct mosquitto* /*mosq*/, void* userdata,
                const struct mosquitto_message* msg) {
  auto* ctx = static_cast<EchoContext*>(userdata);
  if (!ctx) return;

  EchoContext::Clock::time_point now_tp = EchoContext::Clock::now();

  bench_mqtt::ReqHeader req{};
  if (!bench_mqtt::parse_req_payload(msg->payload,
                                     static_cast<std::size_t>(msg->payloadlen),
                                     req)) {
    if (!ctx->args.quiet) {
      std::cerr << "Failed to parse req payload (len=" << msg->payloadlen << ")\n";
    }
    return;
  }

  {
    std::lock_guard<std::mutex> lk(ctx->mu);
    ++ctx->recv_count;
    ctx->last_payload_bytes = static_cast<std::size_t>(msg->payloadlen);
    if (ctx->have_prev) {
      const auto dt =
          std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(now_tp -
                                                                                ctx->prev_tp);
      ctx->interarrival_us.add(dt.count());
    } else {
      ctx->have_prev = true;
    }
    ctx->prev_tp = now_tp;

    if (ctx->have_last_seq && req.seq <= ctx->last_seq) ++ctx->out_of_order;
    ctx->last_seq = req.seq;
    ctx->have_last_seq = true;
  }

  const std::uint64_t srv_recv_ns = steady_now_ns();
  const std::uint64_t srv_send_ns = steady_now_ns();
  std::string ack = bench_mqtt::make_ack_payload(req.seq, srv_recv_ns, srv_send_ns);
  int rc = mosquitto_publish(mosq, nullptr,
                             ctx->args.ack_topic.c_str(),
                             static_cast<int>(ack.size()),
                             ack.data(),
                             ctx->args.qos,
                             false);
  if (rc != MOSQ_ERR_SUCCESS && !ctx->args.quiet) {
    std::cerr << "Failed to publish ACK: " << mosquitto_strerror(rc) << "\n";
  }

  if (!ctx->args.quiet && (req.seq % 1000 == 0)) {
    std::uint64_t total = 0;
    {
      std::lock_guard<std::mutex> lk(ctx->mu);
      total = ctx->recv_count;
    }
    std::cout << "recv seq=" << req.seq << " total=" << total << "\n";
  }
}

void on_connect(struct mosquitto* mosq, void* userdata, int rc) {
  auto* ctx = static_cast<EchoContext*>(userdata);
  if (!ctx) return;
  if (rc != 0) {
    std::cerr << "MQTT connect failed: " << rc << "\n";
    return;
  }
  int sub_rc = mosquitto_subscribe(mosq, nullptr, ctx->args.req_topic.c_str(), ctx->args.qos);
  if (sub_rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "Failed to subscribe to " << ctx->args.req_topic
              << ": " << mosquitto_strerror(sub_rc) << "\n";
  } else if (!ctx->args.quiet) {
    std::cout << "Subscribed to req-topic=" << ctx->args.req_topic
              << " qos=" << ctx->args.qos << "\n";
  }
}

}  // namespace

int main(int argc, char** argv) {
  Args args;
  if (!parse_args(argc, argv, args)) return 2;

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);

  EchoContext ctx;
  ctx.args = args;
  ctx.start_tp = EchoContext::Clock::now();

  int rc = mosquitto_lib_init();
  if (rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "mosquitto_lib_init failed: " << mosquitto_strerror(rc) << "\n";
    return 1;
  }

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

  std::cout << "bench_mqtt_echo_ack host=" << args.host << " port=" << args.port
            << " req_topic=" << args.req_topic << " ack_topic=" << args.ack_topic
            << " qos=" << args.qos << "\n";

  rc = mosquitto_loop_start(mosq);
  if (rc != MOSQ_ERR_SUCCESS) {
    std::cerr << "mosquitto_loop_start failed: " << mosquitto_strerror(rc) << "\n";
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 1;
  }

  while (g_running.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  mosquitto_loop_stop(mosq, true);
  mosquitto_disconnect(mosq);
  mosquitto_destroy(mosq);
  mosquitto_lib_cleanup();

  const auto end_tp = EchoContext::Clock::now();
  const double dur_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_tp - ctx.start_tp).count();

  std::uint64_t recv_count_snapshot = 0;
  std::uint64_t out_of_order_snapshot = 0;
  std::size_t payload_bytes_snapshot = 0;
  OnlineStats interarrival_snapshot{};
  {
    std::lock_guard<std::mutex> lk(ctx.mu);
    recv_count_snapshot = ctx.recv_count;
    out_of_order_snapshot = ctx.out_of_order;
    payload_bytes_snapshot = ctx.last_payload_bytes;
    interarrival_snapshot = ctx.interarrival_us;
  }

  const double msg_per_s =
      (dur_s > 0.0) ? (static_cast<double>(recv_count_snapshot) / dur_s) : 0.0;
  const double mb_per_s =
      (dur_s > 0.0)
          ? ((static_cast<double>(recv_count_snapshot) * payload_bytes_snapshot) / dur_s /
             1024.0 / 1024.0)
          : 0.0;

  const auto old_flags = std::cout.flags();
  const auto old_prec = std::cout.precision();
  std::cout.setf(std::ios::fixed);
  std::cout << std::setprecision(3);

  std::cout << "=== 汇总（MQTT ACK 回声服务端）===\n"
            << "运行时长: " << dur_s << " 秒\n"
            << "收到请求: " << recv_count_snapshot << " 条\n"
            << "处理速率: " << msg_per_s << " 条/秒\n"
            << "吞吐量: " << mb_per_s << " MiB/秒（payload=" << payload_bytes_snapshot
            << " 字节）\n"
            << "乱序请求: " << out_of_order_snapshot << " 条\n";

  if (interarrival_snapshot.n > 0) {
    std::cout << "到达间隔（微秒 us）: 平均 " << interarrival_snapshot.mean
              << "，最小 " << interarrival_snapshot.min_v
              << "，最大 " << interarrival_snapshot.max_v
              << "（约 " << (interarrival_snapshot.max_v / 1000.0) << " ms）\n"
              << "到达间隔抖动（标准差，微秒 us）: " << interarrival_snapshot.stddev()
              << "\n";
  } else {
    std::cout << "到达间隔: 无有效样本\n";
  }

  std::cout.flags(old_flags);
  std::cout.precision(old_prec);

  return 0;
}

