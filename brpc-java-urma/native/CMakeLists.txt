# brpc-java JNI 适配 URMA —— 构建脚本
#
# 产物：libbrpc_java_urma.so
# 依赖：
#   - JDK（JAVA_HOME）
#   - brpc 的编译产物（头文件 + 库），必须带 -DWITH_URMA=ON 编译
#   - protobuf 4.25.1（/usr/local/lib64/libprotobuf.so，含 protoc）
#   - absl 2308.0.0（/usr/local/lib64/libabsl_*.so，60+ 个）
#   - liburma.so（真硬件 URMA SDK，/usr/lib64/liburma.so）
#   - 本目录下的 test.proto（客户端/服务端共享的调用契约，构建时由 protoc 生成 C++ 代码）
#
# 用法：
#   # JAVA_HOME 指向 JDK（编译期 JDK11/21 均可，产物在 JDK21 上运行兼容）
#   export JAVA_HOME=/path/to/jdk          # 用 readlink -f $(which java) 查实际路径
#   cmake -B build -DJAVA_HOME=$JAVA_HOME \
#       -DBRPC_ROOT=/usr/local/brpc               # 方式一：brpc 安装前缀
#       # 或 -DBRPC_DIR=/path/to/brpc-source      # 方式二：brpc 源码树（用其 build/output/）
#       # 省略则自动在 /usr/local、/usr（或 BRPC_ROOT 环境变量）下探测
#   cmake --build build -j
#
# 编译参数照搬 brpc example/ub_performance（已验证能编出 ub_performance_client）：
#   - 关键 define：-DBRPC_WITH_URMA=1 -D__const__=__unused__（缺这个 ForEachChunk 会解析成 int）
#   - include：brpc 头文件目录 + /usr/include/ub/umdk/urma
#   - 链接：libbrpc + 60+ 个 absl .so + protobuf + gflags + leveldb + crypto/ssl + urma
#

cmake_minimum_required(VERSION 3.16)
project(brpc_java_urma CXX)

# C++17：目标环境 protobuf 4.25.1 + absl 20230802 要求 C++17（absl::string_view 用 std::string_view）
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ---------- 依赖发现 ----------

# JDK
find_package(Java REQUIRED)
if(NOT DEFINED ENV{JAVA_HOME} AND NOT JAVA_HOME)
    message(FATAL_ERROR "JAVA_HOME not set. Please pass -DJAVA_HOME=/path/to/jdk")
endif()
if(JAVA_HOME)
    set(JAVA_HOME_PATH ${JAVA_HOME})
else()
    set(JAVA_HOME_PATH $ENV{JAVA_HOME})
endif()
message(STATUS "JAVA_HOME = ${JAVA_HOME_PATH}")

# ---------- brpc 依赖 ----------
# 只需要 brpc 的**编译产物**（头文件 + 库），不需要 brpc 源码树本身。
# 以下三种方式任选其一，优先级从高到低：
#   1. -DBRPC_ROOT=/usr/local/brpc       brpc 安装前缀（内含 include/ 与 lib/）
#   2. -DBRPC_DIR=/path/to/brpc-src      直接用 brpc 源码树里的 build/output/
#   3. 不传：按 BRPC_ROOT 环境变量 / /usr/local / /usr 自动探测
if(BRPC_ROOT)
    set(BRPC_INCLUDE_DIR ${BRPC_ROOT}/include)
    set(BRPC_LIB_DIR     ${BRPC_ROOT}/lib)
    message(STATUS "brpc from BRPC_ROOT = ${BRPC_ROOT}")
elseif(BRPC_DIR)
    set(BRPC_INCLUDE_DIR ${BRPC_DIR}/build/output/include)
    set(BRPC_LIB_DIR     ${BRPC_DIR}/build/output/lib)
    message(STATUS "brpc from BRPC_DIR = ${BRPC_DIR}")
else()
    find_path(BRPC_INCLUDE_DIR NAMES brpc/channel.h
        HINTS ENV BRPC_ROOT
        PATHS /usr/local/include /usr/include)
    message(STATUS "brpc headers auto-detected: ${BRPC_INCLUDE_DIR}")
endif()

if(BRPC_LIB_DIR)
    find_library(BRPC_LIB NAMES brpc PATHS ${BRPC_LIB_DIR} NO_DEFAULT_PATH)
endif()
if(NOT BRPC_LIB)
    find_library(BRPC_LIB NAMES brpc
        HINTS ENV BRPC_ROOT
        PATHS /usr/local/lib /usr/local/lib64 /usr/lib /usr/lib64)
endif()

if(NOT BRPC_INCLUDE_DIR OR NOT BRPC_LIB)
    message(FATAL_ERROR
        "brpc not found. Provide one of:\n"
        "  -DBRPC_ROOT=/path/to/brpc-install-prefix   (contains include/ and lib/)\n"
        "  -DBRPC_DIR=/path/to/brpc-source-tree       (uses build/output/{include,lib})\n"
        "or install brpc into a default prefix (/usr/local).\n"
        "Note: brpc must be built with -DWITH_URMA=ON.")
endif()
message(STATUS "BRPC_INCLUDE_DIR = ${BRPC_INCLUDE_DIR}")
message(STATUS "BRPC_LIB = ${BRPC_LIB}")

# URMA 头文件（实测在 /usr/include/ub/umdk/urma/）
find_path(URMA_INCLUDE_PATH NAMES urma_api.h
    HINTS ENV URMA_ROOT
    PATHS /usr/include /usr/local/include
    PATH_SUFFIXES ub/umdk/urma umdk/urma urma)
if(URMA_INCLUDE_PATH)
    message(STATUS "URMA_INCLUDE_PATH = ${URMA_INCLUDE_PATH}")
    find_library(URMA_LIB NAMES urma
        HINTS ENV URMA_ROOT
        PATH_SUFFIXES lib lib64)
    message(STATUS "URMA_LIB = ${URMA_LIB}")
else()
    message(FATAL_ERROR "urma_api.h not found. Please install UMDK headers.")
endif()

# protobuf（实测装在 /usr/local/lib64/cmake/protobuf，v4.25.1）
find_package(Protobuf REQUIRED)
message(STATUS "Protobuf_INCLUDE_DIRS = ${Protobuf_INCLUDE_DIRS}")
message(STATUS "Protobuf_LIBRARIES = ${Protobuf_LIBRARIES}")

# absl（实测装在 /usr/local/lib64/cmake/absl，v2308.0.0）
find_package(absl REQUIRED)

# JNI
find_package(JNI REQUIRED)

# gflags / leveldb / openssl（brpc 静态库依赖）
find_library(GFLAGS_LIB NAMES gflags PATHS /usr/local/lib /usr/local/lib64 /usr/lib64)
find_library(LEVELDB_LIB NAMES leveldb PATHS /usr/local/lib /usr/local/lib64 /usr/lib64)
find_library(CRYPTO_LIB NAMES crypto PATHS /usr/lib64)
find_library(SSL_LIB NAMES ssl PATHS /usr/lib64)
message(STATUS "GFLAGS_LIB = ${GFLAGS_LIB}")
message(STATUS "LEVELDB_LIB = ${LEVELDB_LIB}")

# ---------- 协议定义 ----------
# test.proto 是客户端与服务端共享的调用契约（PerfTestService.Test 的请求/响应结构），
# 随本模块一起维护；构建时用 protoc 生成 C++ 代码，不依赖 brpc 源码树的构建产物。
set(TEST_PROTO     ${CMAKE_CURRENT_SOURCE_DIR}/test.proto)
set(TEST_PB_DIR    ${CMAKE_CURRENT_BINARY_DIR}/generated)
set(TEST_PB_CC     ${TEST_PB_DIR}/test.pb.cc)
set(TEST_PB_H      ${TEST_PB_DIR}/test.pb.h)

file(MAKE_DIRECTORY ${TEST_PB_DIR})

if(NOT Protobuf_PROTOC_EXECUTABLE)
    if(PROTOBUF_PROTOC_EXECUTABLE)
        set(Protobuf_PROTOC_EXECUTABLE ${PROTOBUF_PROTOC_EXECUTABLE})
    else()
        find_program(Protobuf_PROTOC_EXECUTABLE protoc REQUIRED)
    endif()
endif()
message(STATUS "Protobuf_PROTOC_EXECUTABLE = ${Protobuf_PROTOC_EXECUTABLE}")

add_custom_command(
    OUTPUT ${TEST_PB_CC} ${TEST_PB_H}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --cpp_out=${TEST_PB_DIR}
            -I ${CMAKE_CURRENT_SOURCE_DIR}
            ${TEST_PROTO}
    DEPENDS ${TEST_PROTO}
    COMMENT "Generating test.pb.cc / test.pb.h from test.proto")

# ---------- target ----------

add_library(brpc_java_urma SHARED
    urma_jni_bridge.cpp
    ${TEST_PB_CC})

# ub_performance 的 include 路径 + 生成的 pb 头文件目录
target_include_directories(brpc_java_urma PRIVATE
    ${JNI_INCLUDE_DIRS}
    ${BRPC_INCLUDE_DIR}
    ${URMA_INCLUDE_PATH}
    ${Protobuf_INCLUDE_DIRS}
    ${TEST_PB_DIR}
)

# 照搬 ub_performance 的 defines（关键：-D__const__=__unused__ 不能少）
target_compile_definitions(brpc_java_urma PRIVATE
    BRPC_WITH_URMA=1
    NDEBUG
    __const__=__unused__
)

# 照搬 ub_performance 的 flags（不用 -O3，用 -O2 跟 example 一致）
target_compile_options(brpc_java_urma PRIVATE
    -O2 -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer
)

# 链接：照搬 ub_performance 的 link.txt（libbrpc.a + 全部 absl + protobuf + gflags + leveldb + crypto/ssl + urma + dl）
# absl 用 find_package(absl) 拿到的 imported targets，避免手写 60+ 个 .so 路径
target_link_libraries(brpc_java_urma PRIVATE
    ${BRPC_LIB}
    ${GFLAGS_LIB}
    ${Protobuf_LIBRARIES}
    absl::die_if_null
    absl::flags
    absl::log_initialize
    absl::statusor
    ${LEVELDB_LIB}
    ${CRYPTO_LIB}
    ${SSL_LIB}
    ${URMA_LIB}
    absl::log_internal_check_op
    absl::leak_check
    absl::log_internal_conditions
    absl::log_internal_message
    absl::log_internal_nullguard
    absl::examine_stack
    absl::log_internal_format
    absl::log_internal_proto
    absl::log_internal_log_sink_set
    absl::log_sink
    absl::log_entry
    absl::flags_internal
    absl::flags_marshalling
    absl::flags_reflection
    absl::flags_config
    absl::flags_program_name
    absl::flags_private_handle_accessor
    absl::flags_commandlineflag
    absl::flags_commandlineflag_internal
    absl::log_globals
    absl::log_internal_globals
    absl::raw_hash_set
    absl::hash
    absl::city
    absl::low_level_hash
    absl::hashtablez_sampler
    absl::random_distributions
    absl::random_seed_sequences
    absl::random_internal_pool_urbg
    absl::random_internal_randen
    absl::random_internal_randen_hwaes
    absl::random_internal_randen_hwaes_impl
    absl::random_internal_randen_slow
    absl::random_internal_platform
    absl::random_internal_seed_material
    absl::random_seed_gen_exception
    absl::status
    absl::cord
    absl::cordz_info
    absl::cord_internal
    absl::cordz_functions
    absl::exponential_biased
    absl::cordz_handle
    absl::synchronization
    absl::graphcycles_internal
    absl::kernel_timeout_internal
    absl::time
    absl::civil_time
    absl::time_zone
    absl::crc_cord_state
    absl::crc32c
    absl::crc_internal
    absl::crc_cpu_detect
    absl::bad_optional_access
    absl::stacktrace
    absl::str_format_internal
    absl::strerror
    absl::symbolize
    absl::malloc_internal
    absl::debugging_internal
    absl::demangle_internal
    absl::strings
    absl::int128
    absl::string_view
    absl::throw_delegate
    absl::strings_internal
    absl::base
    absl::spinlock_wait
    absl::bad_variant_access
    absl::raw_logging_internal
    absl::log_severity
    pthread
    dl
    rt
)

# rpath 让 dlopen 能找到 /usr/local/lib64 下的 absl/protobuf
set_target_properties(brpc_java_urma PROPERTIES
    BUILD_RPATH "/usr/local/lib:/usr/local/lib64;$ORIGIN"
    INSTALL_RPATH "/usr/local/lib:/usr/local/lib64;$ORIGIN"
)

# ---------- install ----------
install(TARGETS brpc_java_urma
    LIBRARY DESTINATION lib)
