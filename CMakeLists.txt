cmake_minimum_required(VERSION 3.28)

project(fastest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 导出 compile_commands.json 供 clangd 使用
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# =========================
# Dependencies
# =========================
find_package(Boost CONFIG REQUIRED COMPONENTS asio intrusive system)
find_package(spdlog CONFIG REQUIRED)
find_package(Catch2 3 REQUIRED)

# =========================
# Main executable
# =========================
add_executable(fastest
        src/main.cpp
        src/protocol/memcahed_request.cpp
        src/protocol/memcahed_request.h
        src/server/fastest_server.h
        src/cache/core_cache.h
        src/cache/core_cache.cpp
        src/server/config.h
)

target_link_libraries(fastest
        PRIVATE
        Boost::asio
        Boost::intrusive
        Boost::system
        spdlog::spdlog
)

# =========================
# Tests
# =========================
enable_testing()

add_executable(fastest_tests
        tests/test_client.cpp
)

# 测试代码若 #include "server/fastest_server.h" 等，需要能找到 src/ 下的头文件
target_include_directories(fastest_tests
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(fastest_tests
        PRIVATE
        Boost::asio
        Boost::intrusive
        Boost::system
        spdlog::spdlog
        Catch2::Catch2WithMain
)

add_test(NAME fastest_tests COMMAND fastest_tests)

# =========================
# Platform specific backend
# =========================

if (WIN32)

    message(STATUS "Boost.Asio backend: IOCP")

    # 关键修复：显式声明目标 Windows 版本为 Windows 10 (0x0A00)
    # 否则 Boost.Asio 会回退到 Windows 7 (0x0601)，导致 async_read/async_write
    # 等模板推导失败、std::from_chars 找不到重载等一系列连锁错误。
    target_compile_definitions(fastest PRIVATE
            _WIN32_WINNT=0x0A00
            WINVER=0x0A00
    )
    target_compile_definitions(fastest_tests PRIVATE
            _WIN32_WINNT=0x0A00
            WINVER=0x0A00
    )

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
        # 只让 pkg-config：
        # 1. 检查 liburing >= 2.0
        # 2. 获取 include/lib 路径
        pkg_check_modules(
                LIBURING
                QUIET
                liburing>=2.0
        )
    endif ()

    if (LIBURING_FOUND)
        # 显式寻找静态库 liburing.a，避免链接到 .so
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

        # 传入完整 .a 路径，保证静态链接
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