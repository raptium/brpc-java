/*
 * brpc-java JNI 适配 URMA —— Java 业务入口
 *
 * 同步用法（连通性校验 / 首包测量）：
 *   try (UrmaChannel ch = new UrmaChannel()) {
 *       ch.init("141.61.84.63", 8005, 40000);
 *       byte[] name = new byte[102400];
 *       Arrays.fill(name, (byte) 'r');
 *       UrmaChannel.PerfTestResponse resp = ch.callTestMethod(false, name, null);
 *       // 校验：resp.name.length == 102400
 *   }
 *
 * 异步流水线用法（对标 C++ client 的 thread_num=1 + queue_depth=N）：
 *   try (UrmaChannel ch = new UrmaChannel()) {
 *       ch.init("141.61.84.63", 8005, 40000);
 *       ch.startAsyncReceiver(new AsyncCallback() {
 *           public void onResponse(long rpcId, PerfTestResponse resp, String err,
 *                                  long startNs, long nativeLatencyUs) {
 *               long e2eNs = System.nanoTime() - startNs;   // 端到端（含回收队列）
 *               // nativeLatencyUs 为 Controller::latency_us()，与 C++ client 可直接对比
 *           }
 *       });
 *       for (int i = 0; i < 10000; i++) {
 *           ch.asyncCallTestMethod(false, name, null, i, System.nanoTime());
 *       }
 *       ch.stopAsyncReceiver(5000);
 *   }
 *
 * 资源管理：
 *   - 实现 AutoCloseable，配合 try-with-resources 自动释放
 *   - close() 内部调 native destroy()，释放 C++ brpc::Channel
 *   - destroy() 内部会先停掉结果回收线程（若还在跑）
 *   - nativeHandle = 0 表示已关闭，重复 close 安全
 *   - JDK9+ 用 Cleaner 兜底：忘记 close() 时由 GC 触发 native 销毁
 *
 * 实例模型（对标 C++ client.cpp）：
 *   - C++ 侧每个 PerformanceTest（每个发送线程）各自 new 一个 brpc::Channel，
 *     通道状态互不共享；Java 侧同样一个 UrmaChannel 实例独占一份原生上下文
 *     （channel + 结果队列 + 回收 pthread + in-flight 计数），多实例互不影响
 *   - 需要多个并发发送线程时，按 C++ 的做法每个线程一个 UrmaChannel 实例
 *
 * 线程安全：
 *   - 单个实例建议由单个发送线程调用 asyncCallTestMethod（与 C++ 每线程一个 channel 一致）
 *   - AsyncCallback.onResponse 在 native 回收 pthread 上串行执行，实现方需自行处理并发
 *   - close() 期间不得继续发送
 */
package com.baidu.brpc.urma;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicLong;

public class UrmaChannel implements AutoCloseable {

    static {
        NativeLoader.load("brpc_java_urma");
    }

    /**
     * 原生上下文指针（native 侧 ChannelCtx：brpc::Channel + 结果队列 + 接收线程），
     * 0 表示未初始化或已关闭。对标 C++ PerformanceTest 里的 _channel 成员。
     */
    private long nativeHandle;

    /**
     * 生命周期标记（对标 C++ 里"PerformanceTest 析构后不得再使用"）。
     * closed=true 后所有发送入口立刻抛 IllegalStateException，而不是让 native
     * 侧访问已释放的 channel（C++ 里这是 UB）。
     */
    private volatile boolean closed;

    /**
     * Cleaner：JDK9+ 推荐的资源回收机制，替代 finalize()。
     * 定位与 C++ 一致：C++ 靠作用域析构（RAII）保证"最后发送结束后才 delete channel"，
     * 这里显式 close() 承担同样的职责；Cleaner 只在忘记 close() 时兜底，
     * 前提同样是"此时已无在途请求"。
     */
    private static final Cleaner CLEANER = Cleaner.create();
    private final Cleaner.Cleanable cleanable;

    /**
     * Cleaner 的清理状态：持有 nativeHandle 的快照。
     * 必须是 static 内部类，且不能持有外部 UrmaChannel 引用（否则阻止 GC）。
     * 用 AtomicLong + getAndSet(0) 取"销毁权"：close() 与 Cleaner 线程并发时
     * 只有一个能拿到非 0 指针执行销毁，避免重复释放（Java 侧独有的场景）。
     */
    private static class CleanState implements Runnable {
        private final AtomicLong nativeHandle = new AtomicLong(0);

        /** init() 成功后调用，写入真实 native 指针供 Cleaner 兜底销毁。 */
        void update(long handle) {
            nativeHandle.set(handle);
        }

        /** close() 后调用，置 0 让 Cleaner 跳过 native 销毁（已显式释放）。 */
        void clear() {
            nativeHandle.set(0);
        }

        @Override
        public void run() {
            long handle = nativeHandle.getAndSet(0);
            if (handle != 0) {
                destroyNative(handle);
            }
        }
    }

    private final CleanState cleanState;

    /**
     * 构造时注册 Cleaner 兜底回收。
     * 此时 nativeHandle=0，init() 成功后由 init 调用方调 cleanState.update() 写入真实指针。
     */
    public UrmaChannel() {
        this.cleanState = new CleanState();
        this.cleanable = CLEANER.register(this, cleanState);
    }

    /**
     * 提前触发 URMA 全局初始化（对标 C++ client.cpp:436 GlobalUrmaInitializeOrDie）。
     * 必须在 init() 之前调一次。重复调用安全（once_flag 保护）。
     *
     * 拆出来的目的：让 init() 只测纯建链耗时，对齐 C++ Connect-Latency 口径。
     * URMA 全局初始化要分配 buffer_count 个 buffer（本任务 1048576 个），耗时约 200ms，
     * 如果放在 init() 里会把建链时延污染掉。
     */
    public native void warmupUrma();

    /**
     * 初始化 channel，连接到 server（沿用 C++ 默认选项：protocol=baidu_std、
     * connection_type=single、connect_timeout=timeout）。
     * 注意：调用前应先调 warmupUrma() 完成 URMA 全局初始化，否则 init() 会顺带做这件事，
     * 导致建链时延指标偏大（含 URMA 全局初始化开销）。
     */
    public void init(String ip, int port, int timeoutMs) {
        init(ip, port, timeoutMs, timeoutMs, "baidu_std", "single");
    }

    /**
     * 初始化 channel，选项逐项对应 C++ client.cpp 的 brpc::ChannelOptions：
     * socket_mode 固定 URMA（--use_urma），其余由参数给出。
     *
     * 内部流程：调 native initNative() → 写入 nativeHandle 字段 → 更新 Cleaner 的 cleanState。
     *
     * @param timeoutMs        RPC 超时（对应 --rpc_timeout_ms）
     * @param connectTimeoutMs 建链超时（对应 --connect_timeout_ms），<=0 时取 timeoutMs
     * @param protocol         协议（对应 --protocol，默认 baidu_std）
     * @param connectionType   连接方式（对应 --connection_type，默认 single）
     */
    public synchronized void init(String ip, int port, int timeoutMs, int connectTimeoutMs,
                                  String protocol, String connectionType) {
        if (closed) {
            throw new IllegalStateException("UrmaChannel is closed");
        }
        // 重复 init：先释放上一个 native context，避免 native 侧泄漏
        if (nativeHandle != 0) {
            destroy();
            nativeHandle = 0;
            cleanState.clear();
        }
        long handle = initNative(ip, port, timeoutMs, connectTimeoutMs,
                                 protocol, connectionType);
        this.nativeHandle = handle;
        cleanState.update(handle);
    }

    /**
     * native 初始化，返回 ChannelCtx 指针；失败时抛异常。
     */
    private native long initNative(String ip, int port, int timeoutMs,
                                   int connectTimeoutMs, String protocol, String connectionType);

    /**
     * 对标 C++ client.cpp 的 --client_ignore_oc：
     * 打开后每个请求的 Controller 都会 ignore_eovercrowded()。
     * 需在 init() 之后、开始发送之前调用。
     */
    public native void setIgnoreEovercrowded(boolean ignore);

    /**
     * 同步调用 PerfTestService.Test（用于连通性校验与首包测量）。
     */
    public native PerfTestResponse callTestMethod(
            boolean echoAttachment, byte[] name, byte[] requestAttachment);

    // ===================================================================
    // 异步流水线
    // ===================================================================

    /**
     * 异步回调接口。onResponse 在 native 结果回收 pthread 上执行，
     * 实现方需自行处理并发（不要做阻塞操作，不要抛异常）。
     *
     * 注意：
     *   - resp 在失败时为 null，err 在成功时为 null
     *   - startNs 是 asyncCallTestMethod 传入的纳秒时间戳；
     *     System.nanoTime() - startNs 为 Java 侧端到端时延（含结果队列排队与回调耗时）
     *   - nativeLatencyUs 取自 Controller::latency_us()，与 C++ client.cpp 的
     *     HandleResponse 完全一致，可直接与 C++ 客户端对比
     *   - 绝对不能在 onResponse 里同步等待新的 RPC，会导致死锁
     */
    public interface AsyncCallback {
        void onResponse(long rpcId, PerfTestResponse resp, String err,
                        long startNs, long nativeLatencyUs);
    }

    /**
     * 启动结果回收 pthread。必须在第一次 asyncCallTestMethod 之前调一次。
     * 重复调用安全（已启动则忽略）。
     *
     * @param callback 异步回调，rpc 完成时在 native pthread 上调用
     */
    public native void startAsyncReceiver(AsyncCallback callback);

    /**
     * 异步调用 PerfTestService.Test，立即返回，不阻塞。
     * 对应 C++ client.cpp 的 PerformanceTest::SendRequest()。
     *
     * @param echoAttachment    是否要求 server 回显 attachment
     * @param name              主 payload（对应 proto 的 required bytes name）
     * @param requestAttachment 请求附件，可为 null
     * @param rpcId             调用方分配的 RPC ID，原样回传给 AsyncCallback
     * @param startNs           调用方传入的起点时间戳（System.nanoTime()），原样回传
     * @return                  传入的 rpcId，失败抛异常
     */
    public long asyncCallTestMethod(boolean echoAttachment, byte[] name,
                                    byte[] requestAttachment, long rpcId, long startNs) {
        if (closed || nativeHandle == 0) {
            throw new IllegalStateException("channel not initialized or already closed");
        }
        return asyncCallTestMethod0(echoAttachment, name, requestAttachment, rpcId, startNs);
    }

    private native long asyncCallTestMethod0(boolean echoAttachment, byte[] name,
                                             byte[] requestAttachment, long rpcId, long startNs);

    /**
     * 停止结果回收 pthread。
     * - 先等队列清空（最多 timeoutMs）
     * - 再 join pthread
     * - 释放 callback 的 GlobalRef
     *
     * @param timeoutMs 等待 in-flight 结果清空的最大毫秒数
     */
    public native void stopAsyncReceiver(long timeoutMs);

    /** 关闭 channel，释放 native 资源。可重复调用；关闭后再发送会抛 IllegalStateException。 */
    private native void destroy();

    /**
     * 按指针直接销毁 native 资源（供 Cleaner 兜底调用，不依赖实例字段）。
     * 实现：JNI 桥 Java_com_baidu_brpc_urma_UrmaChannel_destroyNative__J
     */
    private static native void destroyNative(long nativeHandle);

    /**
     * 关闭连接（对标 C++ 作用域结束时析构 PerformanceTest → delete _channel）。
     * 调用前必须保证发送线程已停止且无在途请求；关闭后本实例不可再用。
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (nativeHandle != 0) {
            destroy();
            nativeHandle = 0;
        }
        // 标记 Cleaner 跳过 native 销毁（已显式释放，避免重复 free）
        cleanState.clear();
        // 注销 Cleaner（已 close 后无需再兜底）
        cleanable.clean();
    }

    /** 调用结果 POJO（对标 proto PerfTestResponse） */
    public static class PerfTestResponse {
        public final String cpuUsage;
        public final byte[] name;
        public final long serverProcUs;
        public final byte[] responseAttachment;

        public PerfTestResponse(String cpuUsage, byte[] name,
                                long serverProcUs, byte[] responseAttachment) {
            this.cpuUsage = cpuUsage;
            this.name = name;
            this.serverProcUs = serverProcUs;
            this.responseAttachment = responseAttachment;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PerfTestResponse{");
            sb.append("cpuUsage='").append(cpuUsage).append('\'');
            sb.append(", name.length=").append(name != null ? name.length : 0);
            if (name != null && name.length > 0) {
                sb.append(", name[0]=").append((char) name[0]);
            }
            sb.append(", serverProcUs=").append(serverProcUs);
            sb.append(", responseAttachment.length=")
              .append(responseAttachment != null ? responseAttachment.length : 0);
            if (responseAttachment != null && responseAttachment.length > 0
                    && responseAttachment.length < 100) {
                sb.append(", responseAttachment='").append(new String(responseAttachment)).append('\'');
            }
            sb.append('}');
            return sb.toString();
        }
    }
}
