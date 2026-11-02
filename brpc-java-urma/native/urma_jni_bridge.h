/*
 * brpc-java JNI 适配 URMA —— JNI 桥内部头
 *
 * 职责：把 brpc C++ 的 brpc::Channel（SOCKET_MODE_URMA）封装给 Java 侧调用，
 *      让 brpc-java 作为 client 连 brpc C++ 的 ub_performance_server（URMA 真硬件）。
 *
 * 设计基线：brpc-perf-urma-rdma-scheduler/example/ub_performance/client.cpp
 *   - options.socket_mode = brpc::SOCKET_MODE_URMA        (client.cpp:163)
 *   - brpc::urma::GlobalUrmaInitializeOrDie() 显式调用    (client.cpp:436)
 *   - test::PerfTestService_Stub::Test 同步/异步调用        (client.cpp:188,195)
 *
 * 结构：
 *   - 同步调用（done=nullptr）：一次调用一个响应，用于连通性校验与首包测量
 *   - 异步流水线（done≠nullptr）：对标 C++ client 的 thread_num=1 + queue_depth=N
 *       · done 回调只把结果压入无锁队列，不在 bthread 上调用任何 JNI 函数
 *       · 由独立的结果回收 pthread 把结果回送 Java
 *   - 每个 UrmaChannel 实例独占一份 ChannelCtx（brpc::Channel + 结果队列 +
 *     回收线程 + in-flight 计数），对应 C++ 每个 PerformanceTest 各自持有
 *     brpc::Channel，多实例互不影响；时延取 Controller::latency_us()，
 *     与 C++ HandleResponse 的取数方式一致。
 */
#ifndef BRPC_JAVA_URMA_JNI_BRIDGE_H
#define BRPC_JAVA_URMA_JNI_BRIDGE_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Java 侧 UrmaChannel.java 的 native 方法实现。
 * 命名遵循 JNI 规范：Java_<package>_<Class>_<method>
 *   package = com.baidu.brpc.urma
 *   class   = UrmaChannel
 */

/* 初始化 brpc::Channel 并连接 server（对标 client.cpp PerformanceTest::Init）。
 * - 首次调用会先应用 Java 侧 Flags:* 并调 GlobalUrmaInitializeOrDie()（std::call_once）
 * - new brpc::Channel + Init；socket_mode 固定 URMA，其余选项由参数传入，
 *   逐项对应 C++ ChannelOptions：protocol / connection_type / timeout_ms / connect_timeout_ms
 * - 返回 ChannelCtx*（供 Cleaner 兜底销毁）；失败时抛 java.lang.RuntimeException
 *
 * 对应 Java：public native long initNative(String ip, int port, int timeoutMs,
 *              int connectTimeoutMs, String protocol, String connectionType);
 */
JNIEXPORT jlong JNICALL Java_com_baidu_brpc_urma_UrmaChannel_initNative(
        JNIEnv* env, jobject obj, jstring ip, jint port,
        jint timeoutMs, jint connectTimeoutMs,
        jstring protocol, jstring connectionType);

/* 对标 C++ --client_ignore_oc：设置后每个请求的 Controller 都 ignore_eovercrowded()。
 * 对应 Java：public native void setIgnoreEovercrowded(boolean ignore);
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_setIgnoreEovercrowded(
        JNIEnv* env, jobject obj, jboolean ignore);

/* 调用 PerfTestService.Test（同步）。
 * - 构造 test::PerfTestRequest（echo_attachment + name + request_attachment）
 * - 用 test::PerfTestService_Stub::Test(&cntl, &req, &resp, nullptr) 同步调用
 * - 失败时抛 java.lang.RuntimeException
 * - 成功时返回 com.baidu.brpc.urma.UrmaChannel$PerfTestResponse POJO
 *
 * 对应 Java：public native PerfTestResponse callTestMethod(
 *              boolean echoAttachment, byte[] name, byte[] requestAttachment);
 */
JNIEXPORT jobject JNICALL Java_com_baidu_brpc_urma_UrmaChannel_callTestMethod(
        JNIEnv* env, jobject obj,
        jboolean echoAttachment, jbyteArray jName, jbyteArray jReqAttach);

/* ===================================================================
 * 异步流水线
 * =================================================================== */

/* 启动结果回收 pthread。
 * - 必须在第一次 asyncCallTestMethod 之前调一次
 * - 内部 AttachCurrentThread 长期持有 JNIEnv
 * - callbackRef 是 Java 侧 AsyncCallback 的 GlobalRef（弱引用或强引用均可，由 Java 决定）
 *
 * 对应 Java：public native void startAsyncReceiver(Runnable callback);
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_startAsyncReceiver(
        JNIEnv* env, jobject obj, jobject callbackRef);

/* 异步调用 PerfTestService.Test（对标 client.cpp PerformanceTest::SendRequest）。
 * - 立即返回 rpcId，不阻塞
 * - done 回调跑在 brpc worker bthread 上，只把结果 push 到队列
 * - 结果回收 pthread 从队列 pop 后调 Java AsyncCallback.onResponse(...)
 * - startNs 由 Java 侧传入（System.nanoTime() 起点），用于端到端延迟统计；
 *   回调里的 nativeLatencyUs 取自 Controller::latency_us()，与 C++ 口径一致
 *
 * 对应 Java：private native long asyncCallTestMethod0(
 *              boolean echoAttachment, byte[] name, byte[] requestAttachment,
 *              long rpcId, long startNs);
 */
JNIEXPORT jlong JNICALL Java_com_baidu_brpc_urma_UrmaChannel_asyncCallTestMethod0(
        JNIEnv* env, jobject obj,
        jboolean echoAttachment, jbyteArray jName, jbyteArray jReqAttach,
        jlong rpcId, jlong startNs);

/* 停止结果回收 pthread 并等待 in-flight RPC 完成。
 * - 设 running=false，pthread 退出主循环
 * - 等待队列清空（最多 timeoutMs）
 * - DeleteGlobalRef(callbackRef)
 *
 * 对应 Java：public native void stopAsyncReceiver(long timeoutMs);
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_stopAsyncReceiver(
        JNIEnv* env, jobject obj, jlong timeoutMs);

/* 关闭并释放 brpc::Channel。
 * - delete channel，把 Java 侧 nativeHandle 字段清零
 * - 内部会先调 stopAsyncReceiver（如果 pthread 还在跑）
 *
 * 对应 Java：public native void destroy();
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_destroy(
        JNIEnv* env, jobject obj);

#ifdef __cplusplus
}
#endif

#endif  // BRPC_JAVA_URMA_JNI_BRIDGE_H
