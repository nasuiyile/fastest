#pragma once

#include <boost/asio.hpp>
#include <spdlog/spdlog.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ring_array.h"
#include "type.h"

namespace fastest {
namespace asio = boost::asio;
using asio::ip::tcp;

struct TCPSession {
	// 背压：已分配序号、但还没交给写操作的请求数（backlog）超过高水位时暂停读，降到低水位以下再恢复。
	static constexpr uint32_t kPauseReadBacklog = 2048;
	static constexpr uint32_t kResumeReadBacklog = 256;

	// 注意：resume_read 的初始化依赖 socket，socket 必须声明在 resume_read 之前；
	//      out 引用 ring_array 已发送槽位里的数据，ring_array 需声明在 out 之前（析构时 out 先于 ring_array 销毁）。

	tcp::socket socket;
	asio::steady_timer resume_read; // 背压：当条件变量用，永不到期，cancel() 唤醒
	RingArray ring_array;           // 有序响应；[head, next_id) 同时是在途写引用的响应，写完成前不能释放
	GatherSink out;                 // 写路径：在途写的 buffer 描述和短字段 arena

	uint32_t seq{0};
	std::optional<uint32_t> swallow_seq; // 数据块丢弃完成前，不能发出该序号的错误

	bool closing{false};                // 已决定关闭：不再发送，也不再执行新命令
	bool protocol_error_waiting{false}; // 致命解析错误已入队，等待有序响应排空后关闭
	bool session_exited{false};         // session 协程已退出，允许 writer 回收对象
	bool writing{false};                // 写路径：有 async_write 在途；同一时刻最多一个，期间不能回收 session
	bool read_paused{false};            // 背压：读协程因背压在 resume_read 上等待
	bool kick_pending{false};           // 写路径：已记进 SessionTable 的待写列表，等批次结束统一 kick_write；期间不能回收 session

	explicit TCPSession(tcp::socket &&s)
		: socket(std::move(s)), resume_read(socket.get_executor(), asio::steady_timer::time_point::max()) {
	}

	[[nodiscard]] uint32_t backlog() const noexcept {
		return seq - ring_array.next_id(); // 序号允许回绕，无符号减法即可
	}

	// 写完成（或放弃这次写）后调用：先丢掉对槽位的引用，再释放已发送的槽位。
	void release_sent() noexcept {
		out.clear();
		ring_array.release_sent();
	}
};

// 统计计数：只在测试构建（定义了 FASTEST_TESTING）里存在。生产构建里是空类型，配合 [[no_unique_address]] 不占空间，调用也不产生指令。
struct SessionStats {
#if defined(FASTEST_TESTING)
	uint64_t socket_writes{0}; // 发起的 async_write 次数

	void wrote() noexcept {
		++socket_writes;
	}
#else
	void wrote() noexcept {
	}
#endif
};

/*
 * 本分片的连接表和有序写路径。每个 Worker 一个，只由本线程访问，不用 atomic。
 *
 * 读和写互不等待：响应按序号放进连接的环形数组（deliver），只记下连接，不发起写；批次结束（Worker::BatchScope）时
 * flush_writes() 给每个记下的连接 kick_write 一次，一次写带上该连接所有就绪的响应。从不等 socket，
 * 一个不读响应的慢连接不会拖住同分片的其他连接。
 * 同一连接同一时刻最多一个 async_write 在途；在途期间新就绪的响应留在环形数组，由写完成回调作为下一批发出。
 * 连接的关闭和回收也在这里：session 协程已退出、没有写在途、也不在待写列表里时才从表里删除。
 */
class SessionTable {
public:
	explicit SessionTable(const std::size_t shard_id) noexcept : shard_id_(shard_id) {
	}

	SessionTable(const SessionTable &) = delete;
	SessionTable &operator=(const SessionTable &) = delete;

	// 新连接；编号由调用方分配，不会重复。
	void add(const uint64_t tcp_id, tcp::socket &&socket) {
		sessions_.emplace(tcp_id, std::make_unique<TCPSession>(std::move(socket)));
	}

	// 连接不存在（已回收）时返回 nullptr。
	[[nodiscard]] TCPSession *find(const uint64_t tcp_id) const {
		const auto it = sessions_.find(tcp_id);
		return it == sessions_.end() ? nullptr : it->second.get();
	}

	// 还能接收命令的连接：存在且没有在关闭。返回 nullptr 时调用方直接丢弃命令。
	[[nodiscard]] TCPSession *find_open(const uint64_t tcp_id) const {
		const auto it = sessions_.find(tcp_id);
		if (it == sessions_.end() || !it->second || it->second->closing) {
			return nullptr;
		}
		return it->second.get();
	}

	// 响应按序号入环，只记下连接，等批次结束统一 kick_write。只有连接所属的线程会调用：当前分片命中和回程信封里的跨分片响应。
	void deliver(ResponseMessage message) {
		const uint64_t tcp_id = message.tcp_id;
		auto it = sessions_.find(tcp_id);
		if (it == sessions_.end() || !it->second)
			return;
		auto &session = *it->second;
		if (session.closing)
			return;                                    // 丢弃迟到响应
		session.ring_array.append(std::move(message)); // 顺便省一次拷贝
		// 不在这里发起写：记下连接，批次结束时统一 kick_write，一次写带上这一批所有就绪的响应。
		// 写在途时不用记，写完成回调会把期间就绪的响应作为下一批发出。
		if (!session.writing && !session.kick_pending) {
			session.kick_pending = true;
			dirty_.emplace_back(tcp_id, &session);
		}
	}

	// 批次结束（BatchScope 析构）时调用：本批入过环的连接各 kick_write 一次。
	// kick_write 可能回收当前 session；列表里其他 session 的 kick_pending 仍为 true，不会被回收，指针一直有效。
	void flush_writes() noexcept {
		for (std::size_t i = 0; i < dirty_.size(); ++i) {
			const auto [tcp_id, session] = dirty_[i];
			session->kick_pending = false;
			try {
				if (session->closing) {
					// 防御：作用域内被关闭的连接，kick_write 会直接返回，这里补上回收。
					reclaim_if_idle(tcp_id, *session);
					continue;
				}
				kick_write(tcp_id, *session);
			} catch (const std::exception &e) {
				spdlog::error("[shard {}] kick write failed: {}", shard_id_, e.what());
				close_session(tcp_id, *session);
			}
		}
		dirty_.clear();
	}

	// 过大数据块丢弃完成：放行被扣住的错误响应。
	void finish_swallow(const uint64_t tcp_id) {
		TCPSession *session = find_open(tcp_id);
		if (session == nullptr) {
			return;
		}
		session->swallow_seq.reset();
		kick_write(tcp_id, *session);
	}

	// session 协程已退出、没有写在途、也不在本批待写列表里时才回收；致命解析错误要等有序响应发完（closing）才回收。
	// 调用后 session 可能已被销毁，调用方不能再访问它。
	void reclaim_if_idle(const uint64_t tcp_id, const TCPSession &session) {
		if (session.session_exited && !session.writing && !session.kick_pending &&
		    (session.closing || !session.protocol_error_waiting)) {
			sessions_.erase(tcp_id);
		}
	}

#if defined(FASTEST_TESTING)
	[[nodiscard]] uint64_t socket_writes() const noexcept {
		return stats_.socket_writes;
	}
#endif

private:
	// 一批最多编码这么多就发起写；超大 value 只引用不复制，这里限制的是单次写的时长和 iovec 数量。
	static constexpr std::size_t kMaxBatchBytes = 1 << 20;
	static constexpr std::size_t kMaxBatchResponses = 256;

	// 写路径只入队、不等待：由批次结束（flush_writes）、写完成回调和 finish_swallow 调用。
	// 同一连接同一时刻最多一个 async_write 在途；在途期间新就绪的响应留在环形数组，写完成后作为下一批一起发出。
	// 调用后 session 可能已被回收，调用方不能再访问它。
	void kick_write(const uint64_t tcp_id, TCPSession &session) {
		if (session.writing || session.closing) {
			return;
		}
		bool close_after = false;
		try {
			while (session.out.size_bytes() < kMaxBatchBytes &&
			       session.ring_array.sent_count() < kMaxBatchResponses) {
				// memcached queues an oversized object's error while swallowing its
				// data block. Earlier ready responses may still leave in order.
				if (session.swallow_seq && *session.swallow_seq == session.ring_array.next_id()) {
					break;
				}
				// 直接从槽位编码，不搬出：编码引用的数据留在槽里，写完成后 release_sent() 才释放。
				const RingResponseMessage *message = session.ring_array.front();
				if (!message) {
					break;
				}
				close_after = encode_response(session.out, message->data);
				session.ring_array.advance();
				if (close_after) {
					break; // Quit / 致命错误：之后的响应不再发送
				}
			}
		} catch (const std::exception &e) {
			spdlog::error("[shard {}] response batch failed: {}", shard_id_, e.what());
			session.release_sent();
			close_session(tcp_id, session);
			return;
		}

		if (session.out.empty()) {
			// 没有就绪的响应，或者本批只有 noreply 这类不产生字节的响应。
			session.release_sent();
			if (close_after) {
				close_session(tcp_id, session);
				return;
			}
			maybe_resume_read(session);
			reclaim_if_idle(tcp_id, session);
			return;
		}

		session.writing = true;
		stats_.wrote();
		maybe_resume_read(session);
		// 把buffer里的数据异步写完，然后完成回调。
		asio::async_write(session.socket, session.out.buffers(),
		                  [this, tcp_id, close_after](const boost::system::error_code &ec, std::size_t) {
			                  on_write_complete(tcp_id, ec, close_after);
		                  });
	}

	void on_write_complete(const uint64_t tcp_id, const boost::system::error_code &ec, const bool close_after) {
		const auto it = sessions_.find(tcp_id);
		if (it == sessions_.end() || !it->second) {
			return; // writing 期间不会回收 session，正常不会走到这里
		}
		TCPSession &session = *it->second;
		session.writing = false;
		session.release_sent();
		if (ec) {
			if (ec != asio::error::broken_pipe && ec != asio::error::connection_reset && ec != asio::error::eof &&
			    ec != asio::error::bad_descriptor && ec != asio::error::operation_aborted) {
				spdlog::warn("[shard {}] send failed: {}", shard_id_, ec.message());
			}
			close_session(tcp_id, session);
			return;
		}
		if (close_after || session.closing) {
			close_session(tcp_id, session);
			return;
		}
		kick_write(tcp_id, session); // 写的期间新就绪的响应，作为下一批发出
	}

	// 编码一条响应；字段非法时撤销这条已追加的部分，改发 SERVER_ERROR 占住这个 seq。
	bool encode_response(GatherSink &out, const RingResponseData &data) const {
		const GatherSink::Mark mark = out.mark();
		try {
			return std::visit(
				[&out](const auto &value) -> bool {
					using T = std::remove_cvref_t<decltype(value)>;
					if constexpr (std::is_same_v<T, std::vector<ShardResponse> >) {
						// 多分片：按到达顺序依次输出各分片的 items，不先合并。
						return encode_multi_parts(
							out, value, [](const ShardResponse &part) -> const MultiKeyResponse & {
								return part.command;
							});
					} else {
						return encode_to(out, value);
					}
				},
				data);
		} catch (const std::logic_error &e) {
			// invalid_argument 是它的子类
			spdlog::error("[shard {}] encode failed: {}", shard_id_, e.what());
			out.rollback(mark);
			const ErrorResponse err{.kind = ResponseErrorKind::ServerError, .message = "response encoding failed"};
			return encode_to(out, err);
		}
	}

	static void maybe_resume_read(TCPSession &session) {
		if (session.read_paused && session.backlog() <= TCPSession::kResumeReadBacklog) {
			session.resume_read.cancel();
		}
	}

	// 可重复调用；调用后 session 可能已被回收，调用方不能再访问它。
	void close_session(const uint64_t tcp_id, TCPSession &session) {
		if (!session.closing) {
			session.closing = true;
			boost::system::error_code ec;
			session.socket.shutdown(tcp::socket::shutdown_both, ec);
			session.socket.close(ec);     // 唤醒 session() 中的 async_read_some，并取消在途写
			session.resume_read.cancel(); // 唤醒因背压暂停的读协程
		}
		// 写在途时不回收：取消也会回调 on_write_complete，届时再回收；在那之前 ring_array 的已发送槽位仍可能被引用。
		reclaim_if_idle(tcp_id, session);
	}

	std::unordered_map<uint64_t, std::unique_ptr<TCPSession> > sessions_;
	// 本批入过环、等批次结束统一 kick_write 的连接（会话的 kick_pending 去重）。批次之间总是空的。
	std::vector<std::pair<uint64_t, TCPSession *> > dirty_;
	std::size_t shard_id_; // 只用于日志

	// 统计计数，只在测试构建里存在。
	[[no_unique_address]] SessionStats stats_;
};
} // namespace fastest
