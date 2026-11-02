/*
 * brpc-java JNI 适配 URMA —— JNI 桥实现
 *
 * native 方法：
 *   1. warmupUrma              —— 提前完成 URMA 全局初始化（对齐 C++ main 里的调用）
 *   2. initNative              —— new brpc::Channel + Init，返回 ChannelCtx*
 *   3. setIgnoreEovercrowded   —— 对应 C++ --client_ignore_oc
 *   4. callTestMethod          —— 同步调用 PerfTestService.Test，返回 POJO
 *   5. startAsyncReceiver      —— 启动结果回收 pthread
 *   6. asyncCallTestMethod0    —— 异步调用，立即返回 rpcId
 *   7. stopAsyncReceiver       —— 停止结果回收 pthread
 *   8. destroy / destroyNative —— 释放 ChannelCtx
 *
 * 关键设计：
 *   - URMA 全局初始化用 std::call_once 保护，进程内只执行一次
 *   - 同步调用（done=nullptr）用于连通性校验与首包测量
 *   - 异步调用（done≠nullptr）用 pthread 桥接：
 *       a) done 回调跑在 brpc worker bthread 上，只 push 结果队列；
 *       b) 结果回收 pthread 从队列 pop，在 pthread 上调 JNI 回 Java；
 *       c) JNIEnv 永远不在 bthread 上获取或使用。
 *   - 用 _Stub 而非 CallMethod(nullptr,...)，保证 method_name/service_name 正确写入 RpcMeta
 *   - max_body_size 设大（20GB），与 C++ client 侧配置一致
 *   - done 回调里取 Controller::latency_us()（与 C++ HandleResponse 相同），
 *     随结果回传 Java；Java 侧另记端到端时延，两者之差即回收链路开销
 *
 * 参考实现：brpc-perf-urma-rdma-scheduler/example/ub_performance/client.cpp:155-205
 */

#include "urma_jni_bridge.h"

#include <jni.h>
#include <string>
#include <mutex>
#include <atomic>
#include <thread>
#include <deque>
#include <vector>
#include <chrono>
#include <algorithm>
#include <condition_variable>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/options.pb.h>
#include <brpc/protocol.h>   // brpc::FLAGS_max_body_size
#include <brpc/urma/urma_helper.h>

#include <gflags/gflags.h>   // gflags::SetCommandLineOption

#include "test.pb.h"

namespace {

// URMA 全局初始化的 once_flag（进程级，多 Java 线程并发 init 也只调一次）
std::once_flag g_urma_init_once;

// 全局 JavaVM 指针，JNI_OnLoad 时保存，结果回收 pthread 用它 AttachCurrentThread
JavaVM* g_jvm = nullptr;

// 从 Java 系统 property 读 URMA 配置，转成 gflags 设置。
// 必须在 GlobalUrmaInitializeOrDie 之前调，因为初始化时读这些 flag。
void applyUrmaFlagsFromJavaProperties(JNIEnv* env) {
    static const char* flag_names[] = {
        "use_urma",
        "urma_device",
        "urma_buffer_size",
        "urma_buffer_count",
        "urma_use_polling",
        "urma_poller_num",
        "urma_poller_yield",
        "urma_disable_bthread",
        "urma_post_list_size",
        "urma_recv_block_num",
        "socket_max_unwritten_bytes",
    };

    jclass systemCls = env->FindClass("java/lang/System");
    if (systemCls == nullptr) {
        env->ExceptionClear();
        return;
    }
    jmethodID getProperty = env->GetStaticMethodID(systemCls, "getProperty",
        "(Ljava/lang/String;)Ljava/lang/String;");
    if (getProperty == nullptr) {
        env->ExceptionClear();
        env->DeleteLocalRef(systemCls);
        return;
    }

    for (const char* flag : flag_names) {
        std::string propName = std::string("Flags:") + flag;
        jstring jPropName = env->NewStringUTF(propName.c_str());
        if (jPropName == nullptr) {
            env->ExceptionClear();
            continue;
        }
        jstring jValue = (jstring) env->CallStaticObjectMethod(systemCls, getProperty, jPropName);
        env->DeleteLocalRef(jPropName);
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
            continue;
        }
        if (jValue == nullptr) {
            continue;
        }
        const char* valueC = env->GetStringUTFChars(jValue, nullptr);
        if (valueC != nullptr) {
            gflags::SetCommandLineOption(flag, valueC);
            env->ReleaseStringUTFChars(jValue, valueC);
        }
        env->DeleteLocalRef(jValue);
    }
    env->DeleteLocalRef(systemCls);
}

// -------------------------------------------------------------------
// Java 侧类 / 方法 / 字段 ID 缓存
// -------------------------------------------------------------------
// 只在第一次使用时解析一次（此时处于 Java 调用线程，FindClass 走应用的
// ClassLoader），之后全程复用：
//   1. 避免每个 RPC 都做 FindClass + GetMethodID —— 同步调用 / 结果回收路径
//      上原本每包一次，开销可观；
//   2. 结果回收 pthread 是 AttachCurrentThread 上来的，其 FindClass 使用系统
//      ClassLoader，自定义 ClassLoader 场景下找不到应用类，缓存后不再依赖它。
struct JavaIds {
    jclass    response_cls  = nullptr;   // UrmaChannel$PerfTestResponse
    jmethodID response_ctor = nullptr;   // (Ljava/lang/String;[BJ[B)V
    jfieldID  native_handle = nullptr;   // UrmaChannel.nativeHandle
};

JavaIds    g_ids;
std::mutex g_ids_mtx;

bool ensureJavaIds(JNIEnv* env) {
    std::lock_guard<std::mutex> lk(g_ids_mtx);
    if (g_ids.response_ctor != nullptr && g_ids.native_handle != nullptr) {
        return true;
    }

    if (g_ids.response_cls == nullptr) {
        jclass local = env->FindClass("com/baidu/brpc/urma/UrmaChannel$PerfTestResponse");
        if (local != nullptr) {
            g_ids.response_cls =
                static_cast<jclass>(env->NewGlobalRef(local));
            env->DeleteLocalRef(local);
            g_ids.response_ctor = env->GetMethodID(g_ids.response_cls, "<init>",
                "(Ljava/lang/String;[BJ[B)V");
        }
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
    }

    if (g_ids.native_handle == nullptr) {
        jclass ch_cls = env->FindClass("com/baidu/brpc/urma/UrmaChannel");
        if (ch_cls != nullptr) {
            g_ids.native_handle = env->GetFieldID(ch_cls, "nativeHandle", "J");
            env->DeleteLocalRef(ch_cls);
        }
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
    }

    return g_ids.response_ctor != nullptr && g_ids.native_handle != nullptr;
}

inline void throwRuntimeException(JNIEnv* env, const std::string& msg) {
    jclass exc = env->FindClass("java/lang/RuntimeException");
    if (exc != nullptr) {
        env->ThrowNew(exc, msg.c_str());
        env->DeleteLocalRef(exc);
    }
}

inline void throwIllegalStateException(JNIEnv* env, const std::string& msg) {
    jclass exc = env->FindClass("java/lang/IllegalStateException");
    if (exc != nullptr) {
        env->ThrowNew(exc, msg.c_str());
        env->DeleteLocalRef(exc);
    }
}

// URMA 全局初始化（幂等）。
// warmupUrma 与 initNative 共用同一入口：调用方若跳过 warmupUrma，
// initNative 也会先把 Java 侧 flags 应用上再初始化，而不是让 brpc 用默认
// flag 完成一次性初始化（那样 device / buffer_count 等配置会被静默忽略）。
void initializeUrmaOnce(JNIEnv* env) {
    try {
        std::call_once(g_urma_init_once, [env]() {
            applyUrmaFlagsFromJavaProperties(env);
            brpc::urma::GlobalUrmaInitializeOrDie();
        });
    } catch (const std::exception& e) {
        throwRuntimeException(env, std::string("GlobalUrmaInitializeOrDie failed: ") + e.what());
    } catch (...) {
        throwRuntimeException(env, "GlobalUrmaInitializeOrDie failed: unknown exception");
    }
}

// ===================================================================
// 异步流水线 —— pthread 桥接
// ===================================================================

// 异步结果（在 bthread 上构造，在 pthread 上消费+销毁）
struct AsyncResult {
    int64_t rpc_id;
    bool failed;
    std::string error_text;
    std::string cpu_usage;
    std::string name;
    int64_t server_proc_us;
    std::string response_attachment;
    int64_t start_ns;
    // done 回调里取到的 RPC 时延（微秒），来自 Controller::latency_us()。
    // 与 C++ client.cpp 的取数口径一致，不含结果队列排队与 Java 回调耗时。
    int64_t latency_us;
};

// 结果队列：mutex + deque + condition_variable。
// 生产者是 done 回调（在 bthread 上 push），消费者是结果回收 pthread（pop 后回调 Java），
// 队列元素由消费者释放。
class ResultQueue {
public:
    void push(AsyncResult* r) {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            q_.push_back(r);
        }
        cv_.notify_one();
    }

    AsyncResult* pop(int timeout_ms) {
        std::unique_lock<std::mutex> lk(mtx_);
        if (q_.empty() && timeout_ms > 0) {
            cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                         [this] { return !q_.empty(); });
        }
        if (q_.empty()) return nullptr;
        AsyncResult* r = q_.front();
        q_.pop_front();
        return r;
    }

    size_t size() {
        std::lock_guard<std::mutex> lk(mtx_);
        return q_.size();
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    std::deque<AsyncResult*> q_;
};

// 每个 UrmaChannel 实例独占的原生上下文。
// 对标 C++ client.cpp：每个 PerformanceTest（每个发送线程）各自 new 一个
// brpc::Channel，通道与统计互不共享；Java 侧同样一个 UrmaChannel 实例一份，
// 因此这里不再有全局单例，多实例互不影响。
struct ChannelCtx {
    brpc::Channel* channel = nullptr;

    // 异步接收侧（本实例私有）
    ResultQueue queue;
    std::thread* receiver_thread = nullptr;
    std::atomic<bool> running{false};
    jobject java_callback_ref = nullptr;
    jmethodID on_response_mid = nullptr;
    jclass callback_cls = nullptr;

    // in-flight 计数：发送前 +1，done 回调末尾 -1。
    // destroy() 必须等它归零后再 delete channel，否则 bthread 迟到的回调会访问
    // 已释放的 Controller。
    std::atomic<int64_t> in_flight{0};

    // 对标 C++ client.cpp 的 --client_ignore_oc：
    // 对应每个请求 Controller::ignore_eovercrowded()，需要在建 Controller 时生效，
    // 因此存在 ctx 上，由 JniDoneClosure 构造时读取。
    std::atomic<bool> ignore_eovercrowded{false};

    // 串行化 start/stop/destroy
    std::mutex mtx;

    // 销毁前把没被回收线程消费的结果释放掉，避免 AsyncResult 泄漏
    ~ChannelCtx() {
        AsyncResult* r = nullptr;
        while ((r = queue.pop(0)) != nullptr) {
            delete r;
        }
    }
};

// nativeHandle 字段存的就是 ChannelCtx*（对标 C++ 的 brpc::Channel* 成员）
inline ChannelCtx* getCtx(JNIEnv* env, jobject obj) {
    if (g_ids.native_handle == nullptr) {
        return nullptr;
    }
    return reinterpret_cast<ChannelCtx*>(env->GetLongField(obj, g_ids.native_handle));
}

inline void setCtx(JNIEnv* env, jobject obj, ChannelCtx* ctx) {
    if (g_ids.native_handle == nullptr) {
        return;
    }
    env->SetLongField(obj, g_ids.native_handle, reinterpret_cast<jlong>(ctx));
}

// jstring → std::string（拷贝后立即释放 UTF 缓冲，避免释放后使用）
inline bool toString(JNIEnv* env, jstring jstr, std::string* out) {
    if (jstr == nullptr) {
        return false;
    }
    const char* c = env->GetStringUTFChars(jstr, nullptr);
    if (c == nullptr) {
        return false;
    }
    out->assign(c);
    env->ReleaseStringUTFChars(jstr, c);
    return true;
}

// done 回调（在 brpc worker bthread 上执行）
// ★★★ 关键：这个函数里绝对不调任何 JNI 函数 ★★★
class JniDoneClosure : public google::protobuf::Closure {
public:
    JniDoneClosure(ChannelCtx* ctx, int64_t rpc_id, int64_t start_ns,
                   test::PerfTestRequest* req)
        : ctx_(ctx), rpc_id_(rpc_id), start_ns_(start_ns),
          cntl_(new brpc::Controller()),
          response_(new test::PerfTestResponse()),
          request_(req) {
        // 对标 C++ client.cpp SendRequest(): cntl->ignore_eovercrowded()
        if (ctx != nullptr && ctx->ignore_eovercrowded.load()) {
            cntl_->ignore_eovercrowded();
        }
    }

    void Run() override {
        AsyncResult* r = new AsyncResult();
        r->rpc_id = rpc_id_;
        r->failed = cntl_->Failed();
        r->error_text = cntl_->ErrorText();
        r->start_ns = start_ns_;
        // 与 C++ client.cpp HandleResponse 完全一致的取数方式：用 brpc 自己记录的
        // Controller::latency_us()（SendRPC→EndRPC），不做额外打点。
        r->latency_us = cntl_->latency_us();
        if (!r->failed) {
            r->cpu_usage = response_->cpu_usage();
            if (response_->has_name()) {
                r->name = response_->name();
            }
            r->server_proc_us = response_->has_server_proc_us()
                                ? response_->server_proc_us() : 0;
            r->response_attachment = cntl_->response_attachment().to_string();
        }
        ctx_->queue.push(r);
        delete cntl_;
        delete response_;
        delete request_;
        cntl_ = nullptr;
        response_ = nullptr;
        request_ = nullptr;
        // ★ 必须在 delete cntl_ 之后、delete this 之前递减，
        //   保证 destroy() 等到归零时 bthread 上对 cntl_ 的访问已全部完成。
        ctx_->in_flight.fetch_sub(1, std::memory_order_release);
        delete this;
    }

    // Run() 可能永远不会被调用（例如调用方在发 RPC 前就失败并 delete done），
    // 此时由析构函数释放 Run() 本该释放的资源，避免 Controller/请求/响应泄漏。
    ~JniDoneClosure() override {
        delete cntl_;
        delete response_;
        delete request_;
    }

    brpc::Controller* cntl() { return cntl_; }
    test::PerfTestResponse* response() { return response_; }

private:
    ChannelCtx* ctx_;
    int64_t rpc_id_;
    int64_t start_ns_;
    brpc::Controller* cntl_;
    test::PerfTestResponse* response_;
    test::PerfTestRequest* request_;  // 由 Closure 持有，done 后释放
};

// 结果回收 pthread 主循环（每个 ChannelCtx 一个线程）
// ★ 在 pthread 上调 JNI，JNIEnv 始终有效，不在 bthread 上触碰 JNI ★
void receiverThreadMain(ChannelCtx* ctx) {
    JNIEnv* env = nullptr;
    // JDK 8 的 AttachCurrentThread 签名是 (void**, void*)，必须用 (void**)&env
    if (g_jvm == nullptr || g_jvm->AttachCurrentThread((void**)&env, nullptr) != JNI_OK) {
        return;
    }

    while (ctx->running.load()) {
        AsyncResult* r = ctx->queue.pop(10);
        if (r == nullptr) continue;

        if (ctx->java_callback_ref != nullptr && ctx->on_response_mid != nullptr) {
            jobject jResp = nullptr;
            // 类/构造器 ID 已在 initNative 时缓存，这里不再每包 FindClass
            // （回收线程是 attach 上来的，本身也不适合依赖 FindClass）
            if (!r->failed && g_ids.response_cls != nullptr && g_ids.response_ctor != nullptr) {
                jstring jCpuUsage = env->NewStringUTF(r->cpu_usage.c_str());
                jbyteArray jName = nullptr;
                if (!r->name.empty()) {
                    jName = env->NewByteArray(static_cast<jsize>(r->name.size()));
                    if (jName != nullptr) {
                        env->SetByteArrayRegion(jName, 0,
                            static_cast<jsize>(r->name.size()),
                            reinterpret_cast<const jbyte*>(r->name.data()));
                    }
                }
                jbyteArray jAttach = nullptr;
                if (!r->response_attachment.empty()) {
                    jAttach = env->NewByteArray(static_cast<jsize>(r->response_attachment.size()));
                    if (jAttach != nullptr) {
                        env->SetByteArrayRegion(jAttach, 0,
                            static_cast<jsize>(r->response_attachment.size()),
                            reinterpret_cast<const jbyte*>(r->response_attachment.data()));
                    }
                }
                jResp = env->NewObject(g_ids.response_cls, g_ids.response_ctor,
                                       jCpuUsage, jName,
                                       static_cast<jlong>(r->server_proc_us), jAttach);
                if (jCpuUsage) env->DeleteLocalRef(jCpuUsage);
                if (jName) env->DeleteLocalRef(jName);
                if (jAttach) env->DeleteLocalRef(jAttach);
            }

            jstring jErr = r->failed ? env->NewStringUTF(r->error_text.c_str()) : nullptr;
            env->CallVoidMethod(ctx->java_callback_ref, ctx->on_response_mid,
                                static_cast<jlong>(r->rpc_id), jResp, jErr,
                                static_cast<jlong>(r->start_ns),
                                static_cast<jlong>(r->latency_us));
            if (jErr) env->DeleteLocalRef(jErr);
            if (jResp) env->DeleteLocalRef(jResp);
            if (env->ExceptionCheck()) {
                env->ExceptionClear();
            }
        }

        delete r;
    }

    // 退出前清空队列（不再回调 Java）
    while (true) {
        AsyncResult* r = ctx->queue.pop(0);
        if (r == nullptr) break;
        delete r;
    }

    g_jvm->DetachCurrentThread();
}

}  // namespace

extern "C" {

// JNI_OnLoad：保存 JavaVM 指针
// 返回 JNI_VERSION_10（JDK11 头文件最高支持此常量）。
// 说明：JNI 向后兼容——返回 1.8/9/10 的 .so 在 JDK21 上完全能正常运行，
//       JVM 只用版本号决定启用哪些 JNI 特性，JDK21 JVM 加载声明旧版本的 .so 没问题。
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
    g_jvm = vm;
    return JNI_VERSION_10;
}

/*
 * warmupUrma: 提前触发 URMA 全局初始化（对标 client.cpp:436）。
 * 必须在 init() 之前调一次。重复调用安全（once_flag 保护）。
 * 拆出来的目的：让 init() 只测纯建链耗时，对齐 C++ Connect-Latency 口径。
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_warmupUrma(JNIEnv* env, jobject) {
    initializeUrmaOnce(env);
}

/*
 * initNative: 对标 client.cpp PerformanceTest::Init()（纯建链，URMA 全局初始化已由
 * warmupUrma 提前完成）。ChannelOptions 逐项对应 C++：
 *   socket_mode=URMA / protocol / connection_type / timeout_ms / connect_timeout_ms
 * 返回 ChannelCtx 指针（对标 C++ 的 brpc::Channel* 成员），Java 侧 Cleaner 用它兜底。
 */
JNIEXPORT jlong JNICALL Java_com_baidu_brpc_urma_UrmaChannel_initNative(
        JNIEnv* env, jobject obj, jstring ip, jint port,
        jint timeoutMs, jint connectTimeoutMs,
        jstring protocol, jstring connectionType) {

    if (ip == nullptr) {
        throwRuntimeException(env, "ip is null");
        return 0;
    }
    if (!ensureJavaIds(env)) {
        throwRuntimeException(env, "failed to resolve Java classes of com.baidu.brpc.urma.UrmaChannel");
        return 0;
    }

    // 即使调用方跳过 warmupUrma，也先按 Java 侧 flags 完成 URMA 全局初始化
    initializeUrmaOnce(env);
    if (env->ExceptionCheck()) {
        return 0;
    }

    std::string server_addr;
    if (!toString(env, ip, &server_addr)) {
        return 0;
    }
    std::string proto = "baidu_std";
    std::string conn_type = "single";
    toString(env, protocol, &proto);
    toString(env, connectionType, &conn_type);

    ChannelCtx* ctx = new ChannelCtx();
    ctx->channel = new brpc::Channel();
    brpc::ChannelOptions opt;
    opt.socket_mode        = brpc::SOCKET_MODE_URMA;
    opt.protocol           = proto;
    opt.connection_type    = conn_type;
    opt.timeout_ms         = timeoutMs;
    opt.connect_timeout_ms = connectTimeoutMs > 0 ? connectTimeoutMs : timeoutMs;
    brpc::FLAGS_max_body_size = 21474836480;

    int ret = ctx->channel->Init(server_addr.c_str(), port, &opt);

    if (ret != 0) {
        delete ctx->channel;
        delete ctx;
        throwRuntimeException(env,
            "brpc::Channel::Init failed, ret=" + std::to_string(ret)
            + ", server=" + server_addr);
        return 0;
    }

    setCtx(env, obj, ctx);
    return reinterpret_cast<jlong>(ctx);
}

/*
 * setIgnoreEovercrowded: 对标 C++ client.cpp 的 --client_ignore_oc。
 * 作用在每个请求创建的 Controller 上（JniDoneClosure 构造时读取 ctx 里的标记）。
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_setIgnoreEovercrowded(
        JNIEnv* env, jobject obj, jboolean ignore) {
    ChannelCtx* ctx = getCtx(env, obj);
    if (ctx != nullptr) {
        ctx->ignore_eovercrowded.store(ignore == JNI_TRUE);
    }
}

/*
 * callTestMethod: 同步调用 PerfTestService.Test（连通性校验 / 首包测量）
 */
JNIEXPORT jobject JNICALL Java_com_baidu_brpc_urma_UrmaChannel_callTestMethod(
        JNIEnv* env, jobject obj,
        jboolean echoAttachment, jbyteArray jName, jbyteArray jReqAttach) {

    if (!ensureJavaIds(env)) {
        throwRuntimeException(env, "failed to resolve Java classes of com.baidu.brpc.urma.UrmaChannel");
        return nullptr;
    }

    ChannelCtx* ctx = getCtx(env, obj);
    if (ctx == nullptr || ctx->channel == nullptr) {
        throwIllegalStateException(env, "channel not initialized (nativeHandle == 0)");
        return nullptr;
    }
    brpc::Channel* channel = ctx->channel;

    test::PerfTestRequest request;
    request.set_echo_attachment(echoAttachment == JNI_TRUE);

    if (jName != nullptr) {
        jsize nameLen = env->GetArrayLength(jName);
        if (nameLen > 0) {
            jbyte* nameBuf = env->GetByteArrayElements(jName, nullptr);
            if (nameBuf == nullptr) {
                return nullptr;
            }
            request.set_name(reinterpret_cast<const char*>(nameBuf),
                             static_cast<size_t>(nameLen));
            env->ReleaseByteArrayElements(jName, nameBuf, JNI_ABORT);
        }
    }

    brpc::Controller cntl;
    if (jReqAttach != nullptr) {
        jsize attachLen = env->GetArrayLength(jReqAttach);
        if (attachLen > 0) {
            jbyte* attachBuf = env->GetByteArrayElements(jReqAttach, nullptr);
            if (attachBuf == nullptr) {
                return nullptr;
            }
            cntl.request_attachment().append(reinterpret_cast<const char*>(attachBuf),
                                             static_cast<size_t>(attachLen));
            env->ReleaseByteArrayElements(jReqAttach, attachBuf, JNI_ABORT);
        }
    }

    test::PerfTestResponse response;
    test::PerfTestService_Stub stub(channel);
    stub.Test(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        throwRuntimeException(env,
            "RPC failed: " + cntl.ErrorText()
            + " (log_id=" + std::to_string(cntl.log_id()) + ")");
        return nullptr;
    }

    jstring jCpuUsage = env->NewStringUTF(response.cpu_usage().c_str());
    if (jCpuUsage == nullptr) {
        return nullptr;
    }

    jbyteArray jRespName = nullptr;
    if (response.has_name()) {
        const std::string& nameStr = response.name();
        jRespName = env->NewByteArray(static_cast<jsize>(nameStr.size()));
        if (jRespName == nullptr) {
            env->DeleteLocalRef(jCpuUsage);
            return nullptr;
        }
        if (!nameStr.empty()) {
            env->SetByteArrayRegion(jRespName, 0, static_cast<jsize>(nameStr.size()),
                reinterpret_cast<const jbyte*>(nameStr.data()));
        }
    }

    jlong jServerProcUs = response.has_server_proc_us()
                         ? static_cast<jlong>(response.server_proc_us())
                         : 0;

    jbyteArray jRespAttach = nullptr;
    butil::IOBuf& respAttach = cntl.response_attachment();
    if (!respAttach.empty()) {
        std::string attachStr = respAttach.to_string();
        jRespAttach = env->NewByteArray(static_cast<jsize>(attachStr.size()));
        if (jRespAttach == nullptr) {
            env->DeleteLocalRef(jCpuUsage);
            if (jRespName != nullptr) env->DeleteLocalRef(jRespName);
            return nullptr;
        }
        if (!attachStr.empty()) {
            env->SetByteArrayRegion(jRespAttach, 0, static_cast<jsize>(attachStr.size()),
                reinterpret_cast<const jbyte*>(attachStr.data()));
        }
    }

    jobject result = env->NewObject(g_ids.response_cls, g_ids.response_ctor,
                                    jCpuUsage, jRespName, jServerProcUs, jRespAttach);

    env->DeleteLocalRef(jCpuUsage);
    if (jRespName != nullptr)   env->DeleteLocalRef(jRespName);
    if (jRespAttach != nullptr) env->DeleteLocalRef(jRespAttach);

    return result;
}

// ===================================================================
// 异步流水线：发送 / 接收 / 停止
// ===================================================================

JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_startAsyncReceiver(
        JNIEnv* env, jobject obj, jobject callbackRef) {
    if (!ensureJavaIds(env)) {
        throwRuntimeException(env, "failed to resolve Java classes of com.baidu.brpc.urma.UrmaChannel");
        return;
    }
    ChannelCtx* ctx = getCtx(env, obj);
    if (ctx == nullptr || ctx->channel == nullptr) {
        throwIllegalStateException(env, "channel not initialized (nativeHandle == 0)");
        return;
    }

    std::lock_guard<std::mutex> lk(ctx->mtx);
    if (ctx->running.load()) {
        return;
    }

    if (ctx->callback_cls == nullptr) {
        jclass cls = env->GetObjectClass(callbackRef);
        ctx->callback_cls = (jclass) env->NewGlobalRef(cls);
        env->DeleteLocalRef(cls);
        ctx->on_response_mid = env->GetMethodID(ctx->callback_cls,
            "onResponse",
            "(JLcom/baidu/brpc/urma/UrmaChannel$PerfTestResponse;Ljava/lang/String;JJ)V");
        if (ctx->on_response_mid == nullptr) {
            env->DeleteGlobalRef(ctx->callback_cls);
            ctx->callback_cls = nullptr;
            throwRuntimeException(env, "AsyncCallback.onResponse method not found");
            return;
        }
    }
    if (ctx->java_callback_ref != nullptr) {
        env->DeleteGlobalRef(ctx->java_callback_ref);
    }
    ctx->java_callback_ref = env->NewGlobalRef(callbackRef);

    ctx->running.store(true);
    ctx->receiver_thread = new std::thread(receiverThreadMain, ctx);
}

JNIEXPORT jlong JNICALL Java_com_baidu_brpc_urma_UrmaChannel_asyncCallTestMethod0(
        JNIEnv* env, jobject obj,
        jboolean echoAttachment, jbyteArray jName, jbyteArray jReqAttach,
        jlong rpcId, jlong startNs) {

    if (!ensureJavaIds(env)) {
        throwRuntimeException(env, "failed to resolve Java classes of com.baidu.brpc.urma.UrmaChannel");
        return -1;
    }

    ChannelCtx* ctx = getCtx(env, obj);
    if (ctx == nullptr || ctx->channel == nullptr) {
        throwIllegalStateException(env, "channel not initialized (nativeHandle == 0)");
        return -1;
    }
    if (!ctx->running.load()) {
        throwIllegalStateException(env, "async receiver not started, call startAsyncReceiver first");
        return -1;
    }
    brpc::Channel* channel = ctx->channel;

    test::PerfTestRequest* request = new test::PerfTestRequest();
    request->set_echo_attachment(echoAttachment == JNI_TRUE);

    if (jName != nullptr) {
        jsize nameLen = env->GetArrayLength(jName);
        if (nameLen > 0) {
            jbyte* nameBuf = env->GetByteArrayElements(jName, nullptr);
            if (nameBuf == nullptr) {
                delete request;
                return -1;
            }
            request->set_name(reinterpret_cast<const char*>(nameBuf),
                             static_cast<size_t>(nameLen));
            env->ReleaseByteArrayElements(jName, nameBuf, JNI_ABORT);
        }
    }

    auto* done = new JniDoneClosure(ctx, static_cast<int64_t>(rpcId),
                                    static_cast<int64_t>(startNs), request);
    brpc::Controller* cntl = done->cntl();
    test::PerfTestResponse* response = done->response();

    if (jReqAttach != nullptr) {
        jsize attachLen = env->GetArrayLength(jReqAttach);
        if (attachLen > 0) {
            jbyte* attachBuf = env->GetByteArrayElements(jReqAttach, nullptr);
            if (attachBuf == nullptr) {
                delete done;
                return -1;
            }
            cntl->request_attachment().append(reinterpret_cast<const char*>(attachBuf),
                                              static_cast<size_t>(attachLen));
            env->ReleaseByteArrayElements(jReqAttach, attachBuf, JNI_ABORT);
        }
    }

    // 异步调用：done != nullptr，立即返回
    // ★ done->Run() 将在 brpc worker bthread 上执行，只 push 队列不调 JNI ★
    // ★ 发 RPC 前递增 in-flight 计数，destroy() 据此等待归零再 delete channel，
    //   避免 bthread 迟到回调访问已释放 Controller（2M+qd=30 SIGSEGV 根因）。
    ctx->in_flight.fetch_add(1, std::memory_order_acquire);
    test::PerfTestService_Stub stub(channel);
    stub.Test(cntl, request, response, done);

    return rpcId;
}

// 等待所有 in-flight RPC 的 bthread 回调跑完（Controller 全部 delete 完毕）。
// 必须在 delete channel 之前调用，否则 bthread 仍可能在 EndRPC 中访问已释放 Controller。
// 等待策略：先等 in-flight 归零（bthread 自然完成），再等 queue 排空（receiver 消费完）。
// 两者都有 timeout 兜底，避免极端情况下死等。
static void waitForInFlightAndDrain(ChannelCtx* ctx, int64_t timeoutMs) {
    if (!ctx->running.load()) {
        return;
    }

    int64_t remained = timeoutMs;
    // 阶段 1：等 bthread 上所有 in-flight RPC 完成（Controller 已 delete）
    // 给 in-flight 至少 80% 的时间预算，剩余给 queue 排空
    int64_t inflightDeadline = timeoutMs * 4 / 5;
    int64_t w1 = 0;
    while (ctx->in_flight.load(std::memory_order_acquire) > 0 && w1 < inflightDeadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        w1 += 10;
    }
    remained -= w1;

    // 阶段 2：等 queue 排空（receiver pthread 把结果回送 Java）
    int64_t w2 = 0;
    while (ctx->queue.size() > 0 && w2 < remained) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        w2 += 10;
    }
}

// 停接收线程 + 释放回调全局引用。调用方必须持有 ctx->mtx。
// 注意：bthread 上的 done 回调只 push 自己的队列，不抢 ctx->mtx，
//       所以这里 sleep 等待不会死锁。
static void stopReceiverLocked(JNIEnv* env, ChannelCtx* ctx, int64_t timeoutMs) {
    if (!ctx->running.load()) {
        return;
    }
    waitForInFlightAndDrain(ctx, timeoutMs);

    ctx->running.store(false);
    if (ctx->receiver_thread != nullptr) {
        if (ctx->receiver_thread->joinable()) {
            ctx->receiver_thread->join();
        }
        delete ctx->receiver_thread;
        ctx->receiver_thread = nullptr;
    }
    if (ctx->java_callback_ref != nullptr) {
        env->DeleteGlobalRef(ctx->java_callback_ref);
        ctx->java_callback_ref = nullptr;
    }
}

// 释放一个 ChannelCtx：停接收线程 -> 删除 channel -> 删除 ctx。
// ctx 析构会把队列里未消费的 AsyncResult 一并回收。
static void destroyCtx(JNIEnv* env, ChannelCtx* ctx) {
    if (ctx == nullptr) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(ctx->mtx);
        stopReceiverLocked(env, ctx, 5000);
        if (ctx->callback_cls != nullptr) {
            env->DeleteGlobalRef(ctx->callback_cls);
            ctx->callback_cls = nullptr;
        }
        delete ctx->channel;
        ctx->channel = nullptr;
    }
    delete ctx;
}

JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_stopAsyncReceiver(
        JNIEnv* env, jobject obj, jlong timeoutMs) {
    ChannelCtx* ctx = getCtx(env, obj);
    if (ctx == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lk(ctx->mtx);
    stopReceiverLocked(env, ctx, static_cast<int64_t>(timeoutMs));
}

JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_destroy(
        JNIEnv* env, jobject obj) {
    ChannelCtx* ctx = getCtx(env, obj);
    if (ctx != nullptr) {
        setCtx(env, obj, nullptr);
        destroyCtx(env, ctx);
    }
}

/*
 * destroyNative(long): 静态方法，供 Cleaner 兜底调用（JDK9+ 替代 finalize）。
 * 与 destroy() 的区别：不依赖 UrmaChannel 实例（Cleaner 的 CleanState 不持有实例引用），
 * 直接按 nativeHandle 指针销毁对应的 ChannelCtx。
 * 注意：Cleaner 线程由 JVM 附加，env 可用；此方法在 close() 已显式释放后会被跳过
 *       （Java 侧 cleanState.clear() 把 handle 置 0）。
 */
JNIEXPORT void JNICALL Java_com_baidu_brpc_urma_UrmaChannel_destroyNative__J(
        JNIEnv* env, jclass, jlong nativeHandle) {
    destroyCtx(env, reinterpret_cast<ChannelCtx*>(nativeHandle));
}

}  // extern "C"
