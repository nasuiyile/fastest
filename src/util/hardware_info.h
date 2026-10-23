//
// Created by 12968 on 2026/9/26.
//
# pragma once
#include <thread>
#ifdef __linux__
#include <sched.h>
#endif


namespace util {
// 分片数取本进程可运行的 CPU 数：hardware_concurrency() 返回整机在线 CPU 数，
// 不受 taskset / cgroup cpuset 限制，绑核运行时会让多个分片线程挤在同一个核上。
inline std::size_t core_num() noexcept {
#ifdef __linux__
	cpu_set_t cpus;
	CPU_ZERO(&cpus);
	if (sched_getaffinity(0, sizeof(cpus), &cpus) == 0 && CPU_COUNT(&cpus) > 0)
		return static_cast<std::size_t>(CPU_COUNT(&cpus));
#endif
	unsigned int thread_count = std::thread::hardware_concurrency();
	if (thread_count == 0)
		thread_count = 1;
	return thread_count;
}
}