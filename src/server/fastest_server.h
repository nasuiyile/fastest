// fastest_server.h
#pragma once

#include "worker.h"
#include "shard_registry.h"
#include "../util/hardware_info.h"

#include <memory>
#include <vector>
#include <spdlog/spdlog.h>

namespace fast_server {
class FastestServer {
public:
	explicit FastestServer(const Config cfg) : config_(cfg) {
	}

	// ============================================================
	//                     完整的 start()
	// ============================================================
	void start() {
		// 1. 分片数 = CPU 核数
		const unsigned int thread_count = util::core_num();
		spdlog::info("FastestServer starting with {} worker threads", thread_count);

		// 2. 初始化 ShardRegistry（N 个 mailbox，每个自带 eventfd）
		ShardRegistry::inst().init(thread_count);

		// 3. 创建 worker（每个线程一个 io_context + mailbox + data）
		workers_.reserve(thread_count);
		for (unsigned int i = 0; i < thread_count; ++i) {
			workers_.emplace_back(std::make_unique<Worker>(i, config_));
		}

		// 4. 启动所有 worker 线程
		//    - 主线程本身作为 worker[0] 运行，不再额外空转
		for (unsigned int i = 1; i < thread_count; ++i) {
			workers_[i]->start();
		}

		// 5. 主线程亲自作为 worker[0]
		//    直接调用 Worker::run() 需要它公开；这里用一个小技巧：
		//    把 worker[0] 也放到独立线程，然后主线程等待 join。
		//    或者：把 Worker::run 设为 public，然后主线程直接跑。
		//    —— 下面采用后者的写法（把 run() 放 public）。
		workers_[0]->start();

		spdlog::info("All {} workers started. Press Ctrl+C to stop.", thread_count);

		// 6. 等待所有 worker 退出（stop() 会通过 eventfd/io_.stop() 唤醒它们）
		for (auto &w : workers_) {
			w->join();
		}

		spdlog::info("FastestServer stopped.");
	}

	// 优雅停止（可从信号处理器调用）
	void stop() {
		for (auto &w : workers_) {
			w->request_stop();
		}
	}

private:
	Config config_;
	std::vector<std::unique_ptr<Worker> > workers_;
};
} // namespace fast_server