# brpc-java-urma

brpc-java 侧的 URMA 客户端模块：通过 JNI 复用 brpc C++ 的 `brpc::Channel`
（`SOCKET_MODE_URMA`），让 Java 进程直接访问 URMA 硬件网络，并与
[C++ 侧 brpc](#附依赖的-brpc-代码仓) 的 UB 高性能示例服务端
（`example/ub_performance` 的 `ub_performance_server`）互通。

目标场景：Java 作为 client、C++ 作为 server，按 `queue_depth` 打满 URMA 带宽，
性能指标与 C++ client（`ub_performance_client`）同口径、可横向对比。

<a id="附依赖的-brpc-代码仓"></a>

### 附：依赖的 brpc 代码仓

- 代码仓：<https://atomgit.com/chenchongyu2026/brpc>
- 分支：`perf/urma-rdma-scheduler`

该分支包含 URMA socket mode、`example/ub_performance` 压测示例（含 `test.proto`、
`client.cpp`、`server.cpp`）以及调度相关改动；下文出现的
`brpc-perf-urma-rdma-scheduler/...` 路径均指这份代码仓的本地检出。

---

## 1. 目录结构

```
brpc-java-urma/
├── pom.xml                     # 独立 Maven 模块（暂未注册进 root pom）
├── native/
│   ├── CMakeLists.txt          # libbrpc_java_urma.so 构建脚本
│   ├── urma_jni_bridge.h       # JNI 导出声明
│   ├── urma_jni_bridge.cpp     # JNI 桥实现（ChannelCtx / 结果队列 / 回收线程）
│   ├── test.proto              # 与 UB server/client 共享的 RPC 契约（构建时生成 test.pb.*）
│   └── build/                  # 构建产物目录
└── src/main/java/com/baidu/brpc/urma/
    ├── NativeLoader.java       # libbrpc_java_urma.so 加载器
    ├── UrmaChannel.java        # 对外 API（init / 同步调用 / 异步流水线 / close）
    └── UrmaPerfTestClient.java # 压测客户端（对标 ub_performance_client）
```

---

## 2. 整体架构

```
┌─────────────────────────────── Java 进程 ───────────────────────────────┐
│  UrmaPerfTestClient（或业务代码）                                        │
│      │  asyncCallTestMethod(...)          ▲ onResponse(...)             │
│      ▼                                    │                             │
│  UrmaChannel ── nativeHandle(ChannelCtx*) │                             │
└──────┬────────────────────────────────────┼─────────────────────────────┘
       │ JNI                                │ JNI（仅在此 pthread 上调用）
┌──────▼────────────────────────────────────┴─────────────────────────────┐
│ libbrpc_java_urma.so                                                     │
│                                                                          │
│  发送路径（Java 线程）                                                    │
│    asyncCallTestMethod0                                                   │
│      ├─ new Controller / Request / Response                              │
│      └─ JniDoneClosure(done) ──► brpc::Channel（URMA）                   │
│                                                                          │
│  完成路径（brpc worker bthread）                                          │
│    JniDoneClosure::Run                                                    │
│      ├─ latency_us = cntl->latency_us()                                  │
│      └─ queue.push(AsyncResult)      ← 只入队，绝不调用 JNI               │
│                                                                          │
│  回收路径（每 ChannelCtx 一个 pthread）                                    │
│    receiverThreadMain                                                     │
│      └─ queue.pop() ──► 构造 PerfTestResponse ──► Java AsyncCallback     │
└──────────────────────────────────────────────────────────────────────────┘
```

一个 `UrmaChannel` 对应 native 侧一个 `ChannelCtx`：

```
ChannelCtx
├── brpc::Channel*                        URMA 通道（SOCKET_MODE_URMA）
├── ResultQueue                           done → 回收线程 的结果队列
├── std::thread*                          结果回收 pthread
├── jobject callback_ref                  回调对象的 GlobalRef
├── jmethodID on_response                 回调方法 ID
├── std::atomic<bool> running             接收线程开关
├── std::atomic<int64_t> in_flight        在途请求计数（销毁前等归零）
├── std::atomic<bool> ignore_eovercrowded 对应 --client_ignore_oc
└── std::mutex mtx                        串行化 start / stop / destroy
```

---

## 3. 关键设计思路

### 3.1 为什么用 JNI 封装 brpc::Channel

brpc C++ 的 URMA 支持包含设备初始化、buffer 池、poller、协议封装等整套逻辑。
在 brpc-java 里重新实现一遍等于重写传输层，风险与工作量都不可控；JNI 方案让 Java
复用同一份 C++ 实现，线上行为与 C++ client 天然一致。

### 3.2 为什么结果回收要走独立 pthread

brpc 的 `done` 回调运行在 worker bthread 上：bthread 栈小、不是 Java 线程、
没有可用的 `JNIEnv`；在其中 `AttachCurrentThread` 再回调 Java 会引入线程附着抖动
与栈风险。

因此 `done` 只做一件事——把结果指针压入队列；由每个通道专属的回收 pthread
`AttachCurrentThread` 后调用 Java 回调，`JNIEnv` 在该线程整个生命周期内稳定有效。

### 3.3 实例模型：一个 UrmaChannel = 一个 Channel = 一个发送线程

与 C++ `client.cpp` 对齐：`Test()` 按 `thread_num` 创建多个 `PerformanceTest`，
每个 `PerformanceTest` 各自 `new brpc::Channel`，互不共享状态。

Java 侧同样：每个 `UrmaChannel` 独占 `ChannelCtx`（通道、队列、回收线程、
in-flight 计数）。需要 N 个并发发送线程就创建 N 个 `UrmaChannel`，不做跨实例共享，
也不需要全局锁。

### 3.4 与 brpc 代码仓的依赖边界

本模块依赖的是 **brpc 的编译产物（头文件 + 库）**，而不是 brpc 的源码：

- 通道能力（URMA socket mode、poller、buffer 池）来自 `libbrpc`；
- 调用契约来自 `native/test.proto`（与 UB server/client 同一份定义），构建时由
  `protoc` 生成 C++ 代码；
- CMake 只做 `find_path(brpc/channel.h)` + `find_library(brpc)`：
  可以指向 brpc 的安装前缀（`-DBRPC_ROOT=`）、源码树的 `build/output/`（`-DBRPC_DIR=`），
  也可以让它自动在 `/usr/local`、`/usr` 下探测。

因此升级 brpc 只需换一个前缀重新编译；只有协议结构变化时才需要同步 `test.proto`。

### 3.5 时延口径与 C++ 一致

- **RTT**：`JniDoneClosure::Run()` 读 `cntl->latency_us()`，与 C++
  `HandleResponse` 取的是同一字段，不含结果队列与回调开销；
- **端到端**：Java 侧 `System.nanoTime() - startNs`，包含回收链路；
- 两者之差即"结果队列 + 回调"的开销，用于判断瓶颈在服务端还是 Java 侧。

### 3.6 建链流程与 ChannelOptions 对齐 C++

| C++ | 本模块 |
| --- | --- |
| `ChannelOptions.socket_mode = SOCKET_MODE_URMA` | 固定 URMA |
| `options.protocol` / `connection_type` | `init(..., protocol, connectionType)` |
| `options.timeout_ms` / `connect_timeout_ms` | `init(..., timeoutMs, connectTimeoutMs)` |
| `GlobalUrmaInitializeOrDie()` 在 main 里调一次 | `warmupUrma()` → `std::call_once` |
| `PerformanceTest::Init()` 建链后发首个同步 RPC 校验 | 建链后同步 `callTestMethod` 校验并计时 |
| `max_retry` + 随机抖动重试 | 同名参数，间隔 `interval + rand(0, interval)` |
| `--client_ignore_oc` → `cntl->ignore_eovercrowded()` | `setIgnoreEovercrowded(true)` |

`warmupUrma()` 单独拆出的原因：URMA 全局初始化要分配 `buffer_count` 个 buffer
（示例配置 1048576 个），耗时数百毫秒，必须与"纯建链耗时"分开统计。

### 3.7 生命周期契约

与 C++ 的 RAII 语义一致：

- `close()` 对应 C++ 作用域结束时的 `~PerformanceTest()` → `delete _channel`；
- 前置条件同样是"发送线程已停止、无在途请求"；
- 关闭后再发送抛 `IllegalStateException`（C++ 里是 UB，这里转成确定的异常）；
- `Cleaner` 仅作忘记 `close()` 时的兜底；`CleanState` 用 `AtomicLong` 的
  `getAndSet(0)` 保证 `close()` 与 Cleaner 并发时只有一个真正执行销毁。

---

## 4. 编译

### 4.1 依赖

| 依赖 | 说明 |
| --- | --- |
| brpc | 上文 [附：依赖的 brpc 代码仓](#附依赖的-brpc-代码仓) 的分支，必须带 `-DWITH_URMA=ON` 编译；只需要其头文件与库文件（安装前缀或源码树 `build/output/` 均可） |
| protobuf | 4.25.1（`/usr/local/lib64`），需同时提供 `protoc`，构建时用它由 `test.proto` 生成 C++ 代码 |
| abseil | 20230802 及以上（`/usr/local/lib64/cmake/absl`） |
| URMA SDK | `urma_api.h` 位于 `/usr/include/ub/umdk/urma/`，库 `liburma.so` |
| gflags / leveldb / openssl | brpc 静态库依赖 |
| JDK | 编译期 JDK11 或 JDK21 均可，产物在 JDK21 上运行 |

### 4.2 协议定义 test.proto

`native/test.proto` 是客户端与服务端之间的**调用契约**：`PerfTestService.Test`
的 `PerfTestRequest` / `PerfTestResponse` 结构。JNI 桥需要用同一份 proto 生成
C++ 类，才能在 wire 上和服务端对上（baidu_std 协议的 body 就是 protobuf 序列化结果）：

```proto
message PerfTestRequest {
    required bool  echo_attachment = 1;
    required bytes name            = 2;
};
message PerfTestResponse {
    required string cpu_usage      = 1;
    required bytes  name           = 2;
    optional int64  server_proc_us = 3;
};
service PerfTestService { rpc Test(PerfTestRequest) returns (PerfTestResponse); };
```

构建时 CMake 调 `protoc` 生成 `test.pb.cc/test.pb.h` 到 `build/generated/`，
无需从 brpc 源码树拷贝任何产物。若服务端改了 `test.proto`，把同一份文件同步到
`native/test.proto` 重新编译即可。

### 4.3 编译 native 库

brpc 依赖只需要"编译产物"，三种指向方式任选其一（优先级从高到低）：

```bash
cd brpc-java/brpc-java-urma/native
export JAVA_HOME=/path/to/jdk          # readlink -f $(which java) 可查实际路径

# 方式一：brpc 已安装到某个前缀（推荐，与源码树解耦）
cmake -B build -DJAVA_HOME=$JAVA_HOME -DBRPC_ROOT=/usr/local/brpc

# 方式二：直接使用 brpc 源码树的构建产物（build/output/{include,lib}）
cmake -B build -DJAVA_HOME=$JAVA_HOME -DBRPC_DIR=/path/to/brpc-source

# 方式三：不指定，让其自动在 /usr/local、/usr（或 $BRPC_ROOT）下探测
cmake -B build -DJAVA_HOME=$JAVA_HOME

cmake --build build -j$(nproc)

ls -la build/libbrpc_java_urma.so
nm -D build/libbrpc_java_urma.so | grep -E "initNative|asyncCallTestMethod0|destroyNative|JNI_OnLoad"
```

找不到 brpc 时 CMake 会给出上述三种写法；无需把 brpc 源码放进本工程，
也不会去读 brpc 的源码文件。

### 4.4 编译 Java jar

```bash
cd brpc-java/brpc-java-urma
$JAVA_HOME/bin/javac -d target/classes src/main/java/com/baidu/brpc/urma/*.java
$JAVA_HOME/bin/jar cf target/brpc-java-urma-1.0.0.jar -C target/classes .
```

模块未注册进 root pom（不影响 brpc-java 主干构建），因此不会随 `mvn package` 一起编译。

---

## 5. 部署与运行

### 5.1 原生库查找顺序

1. 环境变量 `BRPC_JAVA_URMA_LIB`（指向 `.so` 绝对路径）
2. `-Djava.library.path=<dir>`
3. `<user.dir>/native/libbrpc_java_urma.so`、`<user.dir>/libbrpc_java_urma.so`
4. JAR 同级 `native/` 目录
5. classpath 内的 `native/libbrpc_java_urma.so`（抽取到临时目录后加载）

运行时还需保证 `liburma.so`、protobuf、absl 可被动态链接器找到
（CMake 已设置 `INSTALL_RPATH=/usr/local/lib:/usr/local/lib64;$ORIGIN`）。

### 5.2 URMA 参数下发

JNI 桥在首次初始化前把 `-DFlags:<name>=<value>` 应用到 brpc 的 gflags，
因此可直接用 JVM 参数下发 URMA 配置，名称与 C++ 命令行 flag 一一对应：

```bash
-DFlags:use_urma=true
-DFlags:urma_device=bonding_dev_0
-DFlags:urma_buffer_size=4096
-DFlags:urma_buffer_count=1048576
-DFlags:urma_use_polling=true
-DFlags:urma_poller_num=2
-DFlags:urma_poller_yield=false
-DFlags:urma_disable_bthread=false
-DFlags:socket_max_unwritten_bytes=2147483648
```

### 5.3 运行压测客户端

```bash
numactl -N 0 -m 0 $JAVA_HOME/bin/java \
  -Djava.library.path=native/build \
  -DFlags:use_urma=true -DFlags:urma_device=bonding_dev_0 \
  -DFlags:urma_buffer_size=4096 -DFlags:urma_buffer_count=1048576 \
  -DFlags:urma_use_polling=true -DFlags:urma_poller_num=2 \
  -DFlags:urma_poller_yield=false -DFlags:urma_disable_bthread=false \
  -DFlags:socket_max_unwritten_bytes=2147483648 \
  -Xms4g -Xmx4g -XX:+UseParallelGC \
  -cp target/brpc-java-urma-1.0.0.jar \
  com.baidu.brpc.urma.UrmaPerfTestClient \
  bench 141.61.84.63 8005 40 10 102400
```

参数：`[bench] <ip> <port> <testSeconds> <queueDepth> <reqSize>
[maxRetry=3] [connectRetryIntervalMs=200] [ignoreOc=false]`

### 5.4 输出指标

```
=== 压测结果 ===            size / queue_depth / cpu_cores / duration / success / failed
                            payload_echo = OK|FAIL（首包回显校验）
=== 性能指标（RTT） ===      avg / p90 / p99 / p999（Controller::latency_us）
                            带宽 / QPS / Server CPU / Client CPU
                            建链时延 / URMA 初始化时延 / 首包时延
=== 回调侧开销 ===          Java 端到端 p90/p99 以及 (端到端 - RTT) 差值
```

---

## 6. Java API 一览

| 方法 | 说明 |
| --- | --- |
| `warmupUrma()` | 提前完成 URMA 全局初始化，需在 `init` 前调用一次 |
| `init(ip, port, timeoutMs)` | 建链（protocol=baidu_std、connection_type=single） |
| `init(ip, port, timeoutMs, connectTimeoutMs, protocol, connectionType)` | 完整选项版本 |
| `setIgnoreEovercrowded(boolean)` | 对应 `--client_ignore_oc` |
| `callTestMethod(echo, name, attach)` | 同步调用，返回 `PerfTestResponse` |
| `startAsyncReceiver(AsyncCallback)` | 启动结果回收线程并注册回调 |
| `asyncCallTestMethod(echo, name, attach, rpcId, startNs)` | 异步发送，立即返回 |
| `stopAsyncReceiver(timeoutMs)` | 停止回收线程并等在途结果 |
| `close()` | 等价于 C++ 析构：停接收 → 删 channel → 释放 ctx |

`AsyncCallback.onResponse(rpcId, resp, err, startNs, nativeLatencyUs)`：

- `resp` 失败时为 `null`，`err` 成功时为 `null`；
- `startNs` 原样回传，用于计算 Java 端到端时延；
- `nativeLatencyUs` 为 `Controller::latency_us()`，与 C++ 指标同口径。

---

## 7. 已知限制

1. **单实例单发送线程**：与 C++ 每线程一个 channel 的用法一致；需要并发发送时创建多个实例。
2. **接收链路开销**：每个响应会构造 `PerfTestResponse` 与两个 `byte[]`
   （name / attachment），10k QPS × 100KB 量级下 GC 压力明显；只关注吞吐时可忽略响应 payload。
3. **发送链路拷贝**：`GetByteArrayElements` 与 protobuf `set_name` 各拷贝一次 100KB；
   如需进一步压榨吞吐，可改用 `GetPrimitiveArrayCritical` 或 direct ByteBuffer。
4. **销毁前置条件**：`close()` 前必须停止发送并等在途请求结束（与 C++ 析构前置条件相同）。
