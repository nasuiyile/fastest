cmake_minimum_required(VERSION 3.28)

project(fastest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost CONFIG REQUIRED COMPONENTS asio)
find_package(spdlog CONFIG REQUIRED)
add_executable(fastest src/main.cpp
        src/cache/core_cache.cpp
        src/cache/core_cache.h)
find_package(unofficial-numa CONFIG REQUIRED)
#设置别名
add_library(NUMA::NUMA ALIAS unofficial::numa::numa)
find_package(magic_enum CONFIG REQUIRED)
find_package(Catch2 3 REQUIRED)
find_package(cachelib CONFIG REQUIRED)
target_link_libraries(fastest
        PRIVATE
        Boost::asio
        spdlog::spdlog
        cachelib
)

# =========================
# TCP echo 测试客户端
# =========================
# 用 Catch2 写的客户端测试：连接到已经在运行的 fastest（127.0.0.1:9000），
# 通过发送/接收数据来验证 main.cpp 里的 echo 逻辑是否正确。
# 不负责拉起 server，跑测试前需要先手动启动 fastest。

enable_testing()

add_executable(fastest_tests tests/test_client.cpp)

target_link_libraries(fastest_tests
        PRIVATE
        Boost::asio
        spdlog::spdlog
        Catch2::Catch2WithMain
        cachelib
)

add_test(NAME fastest_tests COMMAND fastest_tests)

# =========================
# Platform specific backend
# =========================

if (WIN32)

    message(STATUS "Boost.Asio backend: IOCP")

elseif (CMAKE_SYSTEM_NAME STREQUAL "Linux")

    include(CheckCXXSourceCompiles)

    # Boost.Asio io_uring backend 要求 Linux headers >= 5.10
    check_cxx_source_compiles("
        #include <linux/version.h>

        #if LINUX_VERSION_CODE < KERNEL_VERSION(5, 10, 0)
        #error Linux headers too old
        #endif

        int main() {
            return 0;
        }
    " HAVE_LINUX_5_10_HEADERS)

    find_package(PkgConfig QUIET)

    if (PkgConfig_FOUND)

        # 这里只让 pkg-config 帮我们：
        # 1. 检查 liburing >= 2.0
        # 2. 获取 include/lib 路径
        #
        # 不再使用 IMPORTED_TARGET，
        # 因为 PkgConfig::LIBURING 通常会链接到 .so
        pkg_check_modules(
                LIBURING
                QUIET
                liburing>=2.0
        )

    endif ()

    if (LIBURING_FOUND)

        # 显式寻找静态库 liburing.a
        find_library(
                LIBURING_STATIC_LIBRARY
                NAMES liburing.a
                HINTS ${LIBURING_LIBRARY_DIRS}
        )

    endif ()

    if (HAVE_LINUX_5_10_HEADERS
            AND LIBURING_FOUND
            AND LIBURING_STATIC_LIBRARY)

        message(STATUS "Boost.Asio backend: io_uring")
        message(STATUS "Static liburing: ${LIBURING_STATIC_LIBRARY}")

        target_include_directories(fastest
                PRIVATE
                ${LIBURING_INCLUDE_DIRS}
        )

        # 关键：
        # 这里传入的是 /xxx/liburing.a 的完整路径，
        # CMake 就一定静态链接 liburing
        target_link_libraries(fastest
                PRIVATE
                ${LIBURING_STATIC_LIBRARY}
        )

        target_compile_definitions(fastest
                PRIVATE
                BOOST_ASIO_HAS_IO_URING
                BOOST_ASIO_DISABLE_EPOLL
        )

    else ()

        message(STATUS "Boost.Asio backend: epoll")

    endif ()

endif ()