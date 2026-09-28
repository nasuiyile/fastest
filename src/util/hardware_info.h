//
// Created by 12968 on 2026/9/26.
//
# pragma once
#include <thread>


inline unsigned int core_num() {
	unsigned int thread_count = std::thread::hardware_concurrency();
	if (thread_count == 0)
		thread_count = 1;
	return thread_count;
}