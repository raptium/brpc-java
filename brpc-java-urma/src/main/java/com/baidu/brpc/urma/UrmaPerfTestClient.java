/*
 * brpc-java JNI 适配 URMA —— 异步流水线性能压测客户端
 *
 * 对标 C++ ub_performance_client（thread_num=1 + queue_depth=N）：
 * 1 个发送线程 + 信号量控制同时在途请求；native done 回调只入队，
 * 结果回收 pthread 把响应送回 Java 侧统计。
 *
 * 时延口径（关键）：
 *   - RTT：回调里带回来的 Controller::latency_us()，与 C++ client.cpp HandleResponse
 *          同一字段、同一口径，可直接横向对比；
 *   - 端到端：System.nanoTime() - startNs，含回收链路耗时；
 *   两者之差即"结果队列 + 回调"开销，用于判断瓶颈在服务端还是 Java 侧。
 *
 * 建链流程对标 C++ PerformanceTest::Init()：
 *   Channel Init（纯建链计时）→ 立刻发一次同步 RPC 做连通性校验并记录
 *   First-RPC-Latency → 失败按 max_retry 重试、间隔随机抖动。
 *
 * 用法：
 *   java -Djava.library.path=native/build ... UrmaPerfTestClient \
 *       [bench] <ip> <port> <testSeconds> <queueDepth> <reqSize> \
 *       [maxRetry] [connectRetryIntervalMs] [ignoreOc]
 *   默认：141.61.84.63 / 8005 / 40s / qd=10 / 100KB / maxRetry=3 / 200ms / false
 */
package com.baidu.brpc.urma;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.Semaphore;
import com.sun.management.OperatingSystemMXBean;

public class UrmaPerfTestClient {

    public static void main(String[] args) throws Exception {
        String serverIp    = "141.61.84.63";
        int    serverPort  = 8005;
        int    testSeconds = 40;
        int    queueDepth  = 10;
        int    reqSize     = 102400;

        // 兼容 `bench ...` 写法；省略子命令时同样按压测参数解析
        int base = (args.length > 0 && "bench".equals(args[0])) ? 1 : 0;
        if (args.length > base)     serverIp    = args[base];
        if (args.length > base + 1) serverPort  = Integer.parseInt(args[base + 1]);
        if (args.length > base + 2) testSeconds = Integer.parseInt(args[base + 2]);
        if (args.length > base + 3) queueDepth  = Integer.parseInt(args[base + 3]);
        if (args.length > base + 4) reqSize     = Integer.parseInt(args[base + 4]);

        // 可选参数（对标 client.cpp 的 flags）：max_retry / connect_retry_interval / client_ignore_oc
        int     maxRetry              = 3;
        int     connectRetryInterval  = 200;
        boolean ignoreOc              = false;
        if (args.length > base + 5) maxRetry             = Integer.parseInt(args[base + 5]);
        if (args.length > base + 6) connectRetryInterval = Integer.parseInt(args[base + 6]);
        if (args.length > base + 7) ignoreOc             = Boolean.parseBoolean(args[base + 7]);

        runBench(serverIp, serverPort, testSeconds, queueDepth, reqSize,
                 maxRetry, connectRetryInterval, ignoreOc);
    }

    private static void runBench(String serverIp, int serverPort, int testSeconds,
                                 int queueDepth, int reqSize,
                                 int maxRetry, int connectRetryIntervalMs,
                                 boolean ignoreOc) throws Exception {
        // 与 C++ Init() 一致：max_retry 非法时 clamp 到 3
        if (maxRetry < 0 || maxRetry > 1000) {
            System.out.println("[warn] max_retry=" + maxRetry + " 越界，clamp 到 3");
            maxRetry = 3;
        }
        final int rpcTimeoutMs = 40000;
        final int connectTimeoutMs = 40000;

        System.out.println("=== brpc-java URMA 性能压测（异步流水线） ===");
        System.out.println("server           = " + serverIp + ":" + serverPort);
        System.out.println("test_seconds     = " + testSeconds + " s");
        System.out.println("queue_depth      = " + queueDepth);
        System.out.println("thread_num       = 1");
        System.out.println("req_size         = " + reqSize + " bytes");
        System.out.println("max_retry        = " + maxRetry);
        System.out.println("client_ignore_oc = " + ignoreOc);
        System.out.println();

        final byte[] name = new byte[reqSize];
        Arrays.fill(name, (byte) 'r');

        final LongAdder successCount = new LongAdder();
        final LongAdder failedCount = new LongAdder();

        // 延迟样本：原始 long 数组，单写者（native 结果回收线程）
        final LatencyRecorder rttLatenciesUs = new LatencyRecorder(1 << 19);
        final LatencyRecorder e2eLatenciesUs = new LatencyRecorder(1 << 19);

        final AtomicLong firstRpcLatencyUs = new AtomicLong(-1);
        final AtomicLong connectLatencyUs   = new AtomicLong(-1);
        final AtomicLong urmaInitLatencyUs  = new AtomicLong(-1);
        final AtomicBoolean payloadVerified = new AtomicBoolean(false);
        final AtomicReference<String> payloadError = new AtomicReference<String>();
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicLong rpcIdCounter = new AtomicLong(0);

        // Server CPU：滑动窗口保留最近 10 个样本
        final Deque<Double> serverCpuWindow = new ArrayDeque<Double>(16);
        final Object serverCpuLock = new Object();

        // Client CPU：getProcessCpuTime 累计差值（含 .so 内 native pthread）
        final OperatingSystemMXBean osMxBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        final int cpuCores = osMxBean.getAvailableProcessors();
        long clientCpuStartNs = 0;
        long clientCpuTimeStartNs = 0;

        // queue_depth：每发一个 acquire，每收一个 release
        final Semaphore sem = new Semaphore(queueDepth);

        try (UrmaChannel ch = new UrmaChannel()) {
            long tWarm0 = System.nanoTime();
            ch.warmupUrma();
            long tWarm1 = System.nanoTime();
            final long urmaInitUs = (tWarm1 - tWarm0) / 1000;
            urmaInitLatencyUs.set(urmaInitUs);
            System.out.println("[warmup] URMA 全局初始化完成，耗时 " + urmaInitUs
                + " us（一次性开销，不计入建链时延）");

            // 建链：选项与 C++ ChannelOptions 一一对应
            long tInit0 = System.nanoTime();
            ch.init(serverIp, serverPort, rpcTimeoutMs, connectTimeoutMs,
                    "baidu_std", "single");
            long tInit1 = System.nanoTime();
            final long connectUs = (tInit1 - tInit0) / 1000;
            connectLatencyUs.set(connectUs);
            System.out.println("[init] 连接成功，建链时延 = " + connectUs + " us");

            if (ignoreOc) {
                ch.setIgnoreEovercrowded(true);
                System.out.println("[init] client_ignore_oc = true（Controller::ignore_eovercrowded）");
            }

            // 对标 C++ PerformanceTest::Init()：建链后先发一次同步请求做连通性校验，
            // 顺便测 First-RPC-Latency；失败按 max_retry 重试，间隔随机抖动。
            int retryTimes = 0;
            while (true) {
                long tRpc0 = System.nanoTime();
                try {
                    ch.callTestMethod(false, name, null);
                    long firstUs = (System.nanoTime() - tRpc0) / 1000;
                    firstRpcLatencyUs.set(firstUs);
                    System.out.println("[init] 首个 RPC 成功，首包时延 = " + firstUs + " us"
                        + "（重试 " + retryTimes + " 次）");
                    break;
                } catch (RuntimeException e) {
                    if (retryTimes >= maxRetry) {
                        throw new RuntimeException("RPC call failed, exiting...", e);
                    }
                    int randomMs = connectRetryIntervalMs
                        + java.util.concurrent.ThreadLocalRandom.current()
                              .nextInt(Math.max(1, connectRetryIntervalMs));
                    System.out.println("[init] 第 " + retryTimes + " 次 RPC 失败："
                        + e.getMessage() + "，等待 " + randomMs + " ms 后重试");
                    Thread.sleep(randomMs);
                    retryTimes++;
                }
            }

            ch.startAsyncReceiver(new UrmaChannel.AsyncCallback() {
                @Override
                public void onResponse(long rpcId, UrmaChannel.PerfTestResponse resp,
                                       String err, long startNs, long nativeLatencyUs) {
                    long e2eUs = (System.nanoTime() - startNs) / 1000;

                    // 与 C++ HandleResponse 一致：所有响应都计入稳态统计，不再剔除首包
                    rttLatenciesUs.add(nativeLatencyUs);
                    e2eLatenciesUs.add(e2eUs);

                    // 第一个响应顺便校验 payload 回显正确性（demo 用例删除后的功能回归）
                    if (rpcId == 0) {
                        if (err == null && resp != null && resp.name != null) {
                            boolean lenOk = resp.name.length == reqSize;
                            boolean byteOk = resp.name.length > 0 && resp.name[0] == (byte) 'r';
                            payloadVerified.set(lenOk && byteOk);
                            if (!lenOk) {
                                payloadError.set("name.length=" + resp.name.length
                                    + " expected=" + reqSize);
                            } else if (!byteOk) {
                                payloadError.set("name[0] != 'r'");
                            }
                        } else if (err == null) {
                            payloadError.set("response.name == null");
                        }
                    }

                    if (err != null) {
                        failedCount.increment();
                    } else {
                        successCount.increment();
                        if (resp != null && resp.cpuUsage != null && !resp.cpuUsage.isEmpty()) {
                            try {
                                double cpu = Double.parseDouble(resp.cpuUsage) * 100.0;
                                synchronized (serverCpuLock) {
                                    serverCpuWindow.addLast(cpu);
                                    while (serverCpuWindow.size() > 10) serverCpuWindow.removeFirst();
                                }
                            } catch (NumberFormatException ignore) { }
                        }
                    }

                    sem.release();
                }
            });

            clientCpuStartNs = System.nanoTime();
            clientCpuTimeStartNs = osMxBean.getProcessCpuTime();

            System.out.println();
            System.out.println("[bench] 开始压测，持续 " + testSeconds + " 秒...");
            long benchStart = System.nanoTime();

            Thread sender = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (running.get()) {
                        try {
                            sem.acquire();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        if (!running.get()) {
                            sem.release();
                            break;
                        }
                        long rpcId = rpcIdCounter.getAndIncrement();
                        long startNs = System.nanoTime();
                        try {
                            ch.asyncCallTestMethod(false, name, null, rpcId, startNs);
                        } catch (RuntimeException e) {
                            sem.release();
                            failedCount.increment();
                        }
                    }
                }
            });
            sender.setName("urma-bench-sender");
            sender.start();

            Thread.sleep(testSeconds * 1000L);

            running.set(false);
            sem.release(queueDepth);   // 唤醒可能阻塞在 acquire 的 sender
            sender.join();

            long benchEnd = System.nanoTime();
            long benchDurationNs = benchEnd - benchStart;

            System.out.println("[bench] 发送停止，等待 in-flight RPC 完成...");
            long waitDeadline = System.currentTimeMillis() + 10_000;
            while (sem.availablePermits() < queueDepth
                    && System.currentTimeMillis() < waitDeadline) {
                Thread.sleep(10);
            }

            ch.stopAsyncReceiver(5000);

            long clientCpuEndNs = System.nanoTime();
            long clientCpuTimeEndNs = osMxBean.getProcessCpuTime();

            // ================= 统计 =================
            long success = successCount.sum();
            long failed = failedCount.sum();
            long total = success + failed;

            Stats rtt = Stats.of(rttLatenciesUs.sorted());
            Stats e2e = Stats.of(e2eLatenciesUs.sorted());

            double benchSeconds = benchDurationNs / 1_000_000_000.0;
            double qps = success / benchSeconds;
            double throughputMBps = (success * (long) reqSize) / benchSeconds / (1024.0 * 1024.0);

            double serverCpu = 0.0;
            synchronized (serverCpuLock) {
                if (!serverCpuWindow.isEmpty()) {
                    double sum = 0;
                    for (double v : serverCpuWindow) sum += v;
                    serverCpu = sum / serverCpuWindow.size();
                }
            }

            // 进程 CPU / 墙钟 × 100，已等价"占满几个核"，与 bvar process_cpu_usage 口径一致
            double clientCpu = 0.0;
            if (clientCpuEndNs > clientCpuStartNs) {
                long cpuTimeDeltaNs = clientCpuTimeEndNs - clientCpuTimeStartNs;
                long wallDeltaNs = clientCpuEndNs - clientCpuStartNs;
                clientCpu = (double) cpuTimeDeltaNs / wallDeltaNs * 100.0;
            }

            System.out.println();
            System.out.println("=== 压测结果 ===");
            System.out.println("size              = " + reqSize + " bytes");
            System.out.println("thread_num        = 1");
            System.out.println("queue_depth       = " + queueDepth);
            System.out.println("cpu_cores         = " + cpuCores);
            System.out.println("test_duration     = " + String.format("%.3f", benchSeconds) + " s");
            System.out.println("success_count     = " + success);
            System.out.println("failed_count      = " + failed);
            System.out.println("total_count       = " + total);
            System.out.println("payload_echo      = " + (payloadVerified.get() ? "OK" : "FAIL")
                + (payloadError.get() != null ? " (" + payloadError.get() + ")" : ""));
            System.out.println();
            System.out.println("=== 性能指标（RTT，Controller::latency_us，与 C++ client 同口径） ===");
            System.out.println("平均时延 (avg)    = " + String.format("%.0f", rtt.avgUs) + " us");
            System.out.println("p90 时延          = " + rtt.p90Us + " us");
            System.out.println("p99 时延          = " + rtt.p99Us + " us");
            System.out.println("p999 时延         = " + rtt.p999Us + " us");
            System.out.println("带宽 (throughput) = " + String.format("%.3f", throughputMBps) + " MB/s");
            System.out.println("QPS               = " + String.format("%.0f", qps));
            System.out.println("Server CPU        = " + String.format("%.1f", serverCpu) + " %");
            System.out.println("Client CPU        = " + String.format("%.1f", clientCpu) + " %");
            System.out.println("建链时延          = " + connectLatencyUs.get() + " us (纯建链)");
            System.out.println("URMA初始化时延    = " + urmaInitLatencyUs.get() + " us (一次性开销)");
            System.out.println("首包时延 (Init-RPC)= " + firstRpcLatencyUs.get()
                + " us (建链后首个同步 RPC，对标 C++ First-RPC-Latency)");
            System.out.println();
            System.out.println("=== 回调侧开销（Java 端到端，含结果队列 + 回调） ===");
            System.out.println("平均时延 (avg)    = " + String.format("%.0f", e2e.avgUs) + " us");
            System.out.println("p90 时延          = " + e2e.p90Us + " us");
            System.out.println("p99 时延          = " + e2e.p99Us + " us");
            System.out.println("差值 (avg-rtt)    = "
                + String.format("%.0f", e2e.avgUs - rtt.avgUs) + " us");
            System.out.println();

        } catch (Exception e) {
            System.err.println("✗ 压测失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** 分位数统计（输入需已升序）。 */
    private static final class Stats {
        final double avgUs;
        final long p90Us;
        final long p99Us;
        final long p999Us;

        private Stats(double avgUs, long p90Us, long p99Us, long p999Us) {
            this.avgUs = avgUs;
            this.p90Us = p90Us;
            this.p99Us = p99Us;
            this.p999Us = p999Us;
        }

        static Stats of(long[] sorted) {
            if (sorted.length == 0) {
                return new Stats(0, 0, 0, 0);
            }
            long sum = 0;
            for (long v : sorted) sum += v;
            return new Stats((double) sum / sorted.length,
                percentile(sorted, 0.90),
                percentile(sorted, 0.99),
                percentile(sorted, 0.999));
        }

        private static long percentile(long[] sorted, double p) {
            int idx = (int) (sorted.length * p);
            if (idx >= sorted.length) {
                idx = sorted.length - 1;
            }
            return sorted[idx];
        }
    }

    /** 原始 long 延迟样本容器（单写者），避免 Long 装箱与同步开销。 */
    private static final class LatencyRecorder {
        private long[] buf;
        private int size;

        LatencyRecorder(int initialCapacity) {
            this.buf = new long[Math.max(16, initialCapacity)];
        }

        void add(long value) {
            if (size == buf.length) {
                buf = Arrays.copyOf(buf, buf.length << 1);
            }
            buf[size++] = value;
        }

        int size() {
            return size;
        }

        long[] sorted() {
            long[] copy = Arrays.copyOf(buf, size);
            Arrays.sort(copy);
            return copy;
        }
    }
}
