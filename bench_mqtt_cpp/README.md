## MQTT 1kHz RTT 压测（C++，Mosquitto）

本目录为 **MQTT（Mosquitto broker）** 局域网高频数据压测工具，用于在两台电脑间以 **1kHz** 发送 **1KB** 载荷，并统计 **RTT 延迟**、**到达间距（抖动）**、吞吐与可靠性等指标。协议与统计逻辑尽量对齐 `bench_cpp` 下的 Zenoh 版本，便于横向对比。

### 可执行文件

| 程序 | 作用 |
|------|------|
| `bench_mqtt_echo_ack` | 订阅请求 topic，收到后立即发布 ACK；统计到达间距、吞吐、乱序。 |
| `bench_mqtt_pub_rtt` | 按 1kHz 发送请求，订阅 ACK，统计 RTT（含百分位）、超时、吞吐。 |

### 默认 Topic

- 请求：`demo/mqtt/bench/req`
- ACK：`demo/mqtt/bench/ack`

可通过 `--req-topic`、`--ack-topic` 覆盖。

---

## 环境要求

- CMake ≥ 3.10，C++17
- 已安装 Mosquitto broker 与 C 客户端库：
  - **Ubuntu/Debian**：
    - `sudo apt install mosquitto mosquitto-clients libmosquitto-dev`
  - **macOS/Homebrew**：
    - `brew install mosquitto`
- 运行压测时需有可达的 **Mosquitto broker**（建议运行在 Ubuntu 上，监听 `0.0.0.0:1883`）。

---

## 构建

在仓库根目录执行：

```bash
cmake -S bench_mqtt_cpp -B build/bench_mqtt_cpp
cmake --build build/bench_mqtt_cpp -j
```

生成的可执行文件位于 `build/bench_mqtt_cpp/`：

- `build/bench_mqtt_cpp/bench_mqtt_echo_ack`
- `build/bench_mqtt_cpp/bench_mqtt_pub_rtt`

---

## 运行步骤

### 1. 在 Ubuntu 上启动 Mosquitto broker

安装并启动：

```bash
sudo apt install mosquitto mosquitto-clients libmosquitto-dev
sudo systemctl enable --now mosquitto
```

或前台调试模式：

```bash
mosquitto -v
```

默认监听 `0.0.0.0:1883`。

### 2. 启动接收端（回 ACK + 统计到达间距）

在 **接收端机器**（可以是 broker 所在的 Ubuntu，也可以是 macOS）运行：

```bash
./build/bench_mqtt_cpp/bench_mqtt_echo_ack --host 127.0.0.1 --port 1883
```

跨机器测试时，将 `--host` 改为 broker 所在 Ubuntu 机器的 IP，例如：

```bash
./build/bench_mqtt_cpp/bench_mqtt_echo_ack --host 192.168.1.100 --port 1883
```

常用参数：

- `--req-topic`：请求 topic（默认 `demo/mqtt/bench/req`）
- `--ack-topic`：ACK topic（默认 `demo/mqtt/bench/ack`）
- `--qos`：MQTT QoS 等级（0/1/2，默认 1）
- `--quiet`：减少日志输出

### 3. 启动发送端（1kHz 发送 + RTT 统计）

在 **发送端机器**（推荐另一台 macOS/Ubuntu）运行。

固定条数测试示例：

```bash
./build/bench_mqtt_cpp/bench_mqtt_pub_rtt \
  --host <broker_ip> --port 1883 \
  --rate-hz 1000 --count 100000 \
  --payload-bytes 1024 \
  --ack-timeout-ms 100 \
  --qos 1
```

按时长测试示例（默认 10 秒）：

```bash
./build/bench_mqtt_cpp/bench_mqtt_pub_rtt \
  --host <broker_ip> --port 1883 \
  --rate-hz 1000 --duration-sec 10 \
  --payload-bytes 1024 \
  --ack-timeout-ms 100 \
  --qos 1
```

主要参数：

- `--host` / `--port`：Mosquitto broker 地址，默认 `127.0.0.1:1883`
- `--req-topic` / `--ack-topic`：请求与 ACK topic
- `--rate-hz`：发送频率（Hz），默认 1000
- `--payload-bytes`：每条载荷字节数（默认 1024，必须 ≥ `sizeof(ReqHeader)`）
- `--count`：发送总条数（设定后优先于 `--duration-sec`）
- `--duration-sec`：发送时长（秒），`--count` 未设时生效
- `--ack-timeout-ms`：ACK 超时阈值（毫秒），超时计为 `timeouts`
- `--qos`：MQTT QoS 等级（0/1/2，默认 1）
- `--quiet`：减少日志输出

### 4. 结束与查看结果

- 发送端在发完 `--count` 条或到达 `--duration-sec` 后会自动打印 summary 并退出（也可 `Ctrl+C` 提前结束）。
- 接收端需在发送端结束后按 **Ctrl+C** 退出，退出时会打印 summary。

两端输出的指标结构和含义与 `bench_cpp` 基本一致，只是说明文字中标注为 “MQTT”。可以直接对比 Zenoh 版与 MQTT 版的：

- RTT 平均值、P95/P99、最大值、标准差
- 接收端到达间隔抖动与最大值
- 吞吐量（MiB/s）和超时/丢包比例

