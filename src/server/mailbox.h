#pragma once

#include <boost/lockfree/mpsc_weak_queue.hpp>

#include <sys/eventfd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <system_error>
#include <type_traits>
#include <unistd.h>

namespace fast_server {
enum class ShardOp : uint8_t {
	Get,
	Set,
	Del
};

enum class ShardMessageType : uint8_t {
	Request,
	Response
};

enum class ShardStatus : uint8_t {
	Ok,
	NotFound,
	Error
};

/*
 * 真正的数据放在堆上的 ShardMessage 中。
 *
 * lock-free queue 里只传一个指针，因此 ShardEvent 本身保持
 * trivially-copyable。
 *
 * ownership:
 *
 *   producer:
 *       unique_ptr<ShardMessage>
 *
 *   Mailbox::try_push() 成功:
 *       ownership -> mailbox queue
 *
 *   Mailbox::pop():
 *       ownership -> consumer unique_ptr
 */
struct ShardMessage {
	ShardMessageType type{ShardMessageType::Request};
	ShardOp op{ShardOp::Get};
	ShardStatus status{ShardStatus::Error};

	uint64_t req_id{0};
	uint64_t origin_shard{0};

	uint32_t item_flags{0};

	std::string key;
	std::string value;
};

struct ShardEvent {
	ShardMessage *message{nullptr};
};

static_assert(std::is_trivially_copyable_v<ShardEvent>);

constexpr std::size_t kMailboxCapacity = 4096;

using MailboxQueue =
boost::lockfree::mpsc_weak_queue<
	ShardEvent,
	boost::lockfree::capacity<kMailboxCapacity>
>;

struct alignas(64) Mailbox {
public:
	Mailbox() {
		event_fd_ = ::eventfd(
			0,
			EFD_NONBLOCK | EFD_CLOEXEC
			);

		if (event_fd_ < 0) {
			throw std::system_error(
				errno,
				std::generic_category(),
				"eventfd"
				);
		}
	}

	~Mailbox() {
		/*
		 * 正常 shutdown 顺序应该是：
		 *
		 *   request_stop workers
		 *   join workers
		 *   destroy ShardRegistry / Mailbox
		 *
		 * 这里仍然把残留消息释放掉，避免 shutdown 时泄漏。
		 */
		ShardEvent ev{};

		while (queue_.pop(ev)) {
			delete ev.message;
			ev.message = nullptr;
		}

		if (event_fd_ >= 0) {
			::close(event_fd_);
			event_fd_ = -1;
		}
	}

	Mailbox(const Mailbox &) = delete;

	Mailbox &operator=(const Mailbox &) = delete;

	/*
	 * 非阻塞 push。
	 *
	 * 重点：这里绝对不能在 shard event-loop 线程里面无限自旋。
	 *
	 * 如果 queue 满，直接返回 false。
	 * Worker 会通过 coroutine timer 做退避重试。
	 */
	bool try_push(std::unique_ptr<ShardMessage> &message) {
		if (!message) {
			return false;
		}

		ShardEvent ev{};
		ev.message = message.get();

		if (!queue_.push(ev)) {
			return false;
		}

		/*
		 * queue 已经拥有 message。
		 */
		message.release();

		notify();
		return true;
	}

	/*
	 * consumer 取得 ownership。
	 */
	bool pop(std::unique_ptr<ShardMessage> &out) {
		ShardEvent ev{};

		if (!queue_.pop(ev)) {
			return false;
		}

		out.reset(ev.message);
		return true;
	}

	/*
	 * stream_descriptor 必须拥有自己的 fd。
	 *
	 * 不能直接：
	 *
	 *   stream_descriptor sd(exec, event_fd_);
	 *
	 * 否则 Mailbox 和 stream_descriptor 都会认为自己拥有同一个 fd。
	 */
	int duplicate_event_fd() const noexcept {
#ifdef F_DUPFD_CLOEXEC
		return ::fcntl(
			event_fd_,
			F_DUPFD_CLOEXEC,
			0
			);
#else
		const int fd = ::dup(event_fd_);

		if (fd >= 0) {
			const int flags = ::fcntl(fd, F_GETFD);

			if (flags >= 0) {
				::fcntl(
					fd,
					F_SETFD,
					flags | FD_CLOEXEC
					);
			}
		}

		return fd;
#endif
	}

	/*
	 * eventfd 非 semaphore 模式下，一次 read 会读取整个 counter，并把 counter 清零。
	 */
	void clear_notify() const noexcept {
		uint64_t value = 0;
		while (true) {
			const ssize_t n = ::read(event_fd_, &value, sizeof(value));
			if (n == static_cast<ssize_t>(sizeof(value))) {
				return;
			}
			if (n < 0 && errno == EINTR) {
				continue;
			}
			/*
			 * EAGAIN 表示已经被清掉。
			 *
			 * shutdown 阶段的其他错误也没必要继续 retry。
			 */
			return;
		}
	}

private:
	//通过fd唤醒其他线程来读取队列
	void notify() const noexcept {
		constexpr uint64_t one = 1;
		while (true) {
			//给eventfd计数器+1
			const ssize_t n = ::write(event_fd_, &one, sizeof(one));
			if (n == static_cast<ssize_t>(sizeof(one))) {
				return;
			}
			if (n < 0 && errno == EINTR) {
				continue;
			}
			/*
			 * EAGAIN 的情况意味着 eventfd counter 已经非常大，
			 * 此时 fd 本身已经处于 readable 状态，所以不用继续写。
			 *
			 * 其他错误通常只会发生在 shutdown / 生命周期错误阶段。
			 */
			return;
		}
	}

private:
	MailboxQueue queue_;
	int event_fd_{-1};
};
} // namespace fast_server