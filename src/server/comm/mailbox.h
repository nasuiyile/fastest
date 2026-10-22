#pragma once
#include <boost/asio/detail/config.hpp> // BOOST_ASIO_HAS_IO_URING_AS_DEFAULT
#include <boost/lockfree/mpsc_weak_queue.hpp>

#include <sys/eventfd.h>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <memory>
#include <system_error>
#include <type_traits>
#include <unistd.h>

// 事件只持有指向消息的指针，因此与消息类型无关存储布局
template <typename Message>
struct ShardEvent {
	Message *message {nullptr};
};

constexpr std::size_t kMailboxCapacity = 1024;

namespace fastest {
// 相邻行预取器按 128B 成对拉取缓存行，门铃标志独占一对，避免和队列的 head/tail 伪共享。
inline constexpr std::size_t kCacheLinePair = 128;

/*
 * 门铃：每个分片一个，请求、响应两个 mailbox 共用（一个 eventfd + 一个标志）。
 *
 * rung_ 为 true 表示门铃已抬起，生产者 push 之后不必写 eventfd，因为下面两种情况必居其一：
 *   - 有人敲过门，eventfd 里有计数，消费者会醒；
 *   - 消费者醒着（有流量时一直醒着），睡之前一定会 lower() 并再查一遍队列。
 * rung_ 为 false 只出现在消费者准备睡、或已经睡在 fd 上的时候，之后第一个 push 的生产者负责敲门。
 *
 * 不丢唤醒（Dekker 式）：生产者「push → fence → 读 rung_」，消费者「rung_ = false → fence → 读队列」。
 * 两个 seq_cst fence 总有先后：生产者的在前，消费者的复查一定能看到这条消息；消费者的在前，生产者读到的
 * 要么是 false（自己去敲门），要么是这次放下之后别人写的 true：敲门者写的，它会敲门；消费者 raise() 写的，
 * 消费者醒着，睡前还会再放下、再复查一次。
 * Vyukov 队列的 push 分两步（交换尾指针 → 链接前驱），另一个生产者卡在两步之间时 pop 会提前返回 false，
 * 排在它后面的消息暂时取不到；卡住的生产者链接完成后同样会执行上面的检查，所以也不会丢。
 */
class alignas(kCacheLinePair) Doorbell {
public:
	Doorbell() {
#if defined(BOOST_ASIO_HAS_IO_URING_AS_DEFAULT)
		// io_uring：消费者直接 async_read 门铃，读本身就清零计数，醒来不再额外 read 一次。
		// 用阻塞 fd：O_NONBLOCK 的 fd 在 io_uring 上读不到数据会返回 EAGAIN，asio 再改成 poll + read 系统调用，
		// 多一个来回；阻塞 fd 由内核挂起等待（较老内核的 eventfd 不支持 nowait 读时会交给 io-wq 线程，仍然正确）。
		// 生产者 write 只在计数接近 2^64 时才会阻塞，而每次放下门铃后最多一次敲门，不会发生。
		constexpr int flags = EFD_CLOEXEC;
#else
		constexpr int flags = EFD_NONBLOCK | EFD_CLOEXEC;
#endif
		event_fd_ = ::eventfd(0, flags);
		if (event_fd_ < 0) {
			throw std::system_error(errno, std::generic_category(), "eventfd");
		}
	}

	~Doorbell() {
		if (event_fd_ >= 0) {
			::close(event_fd_);
			event_fd_ = -1;
		}
	}

	Doorbell(const Doorbell &) = delete;
	Doorbell &operator=(const Doorbell &) = delete;

	// ---- 生产者 ----

	// 在一次或多次 push 之后调用。要敲多个门铃时，先调用一次 fence()，再逐个 knock()。
	static void fence() noexcept {
		std::atomic_thread_fence(std::memory_order_seq_cst);
	}

	// 调用前必须先 fence()。返回是否真的写了 eventfd。
	bool knock() noexcept {
		// 快路径只读：消费者醒着时标志所在的缓存行保持共享态，生产者之间不抢写。
		if (rung_.load(std::memory_order_relaxed)) {
			return false;
		}
		// 多个生产者同时看到 false 时只有一个敲门。
		if (rung_.exchange(true, std::memory_order_relaxed)) {
			return false;
		}
		notify();
		return true;
	}

	bool ring() noexcept {
		fence();
		return knock();
	}

	// ---- 消费者（只有门铃所属的分片线程）----

	// 准备睡：放下门铃，随后必须再查一遍队列，查到消息就 raise() 继续处理，确实为空才能等 fd。
	void lower() noexcept {
		rung_.store(false, std::memory_order_relaxed);
		fence();
	}

	// 复查发现了消息：重新抬起门铃，继续醒着处理，期间生产者不必再敲。
	void raise() noexcept {
		rung_.store(true, std::memory_order_relaxed);
	}

	/*
	 * eventfd 非 semaphore 模式下，一次 read 会读取整个 counter，并把 counter 清零。
	 * 只在 fd 可读后由消费者调用（epoll 后端：async_wait 之后）。
	 */
	void clear() const noexcept {
		uint64_t value = 0;
		while (true) {
			const ssize_t n = ::read(event_fd_, &value, sizeof(value));
			if (n < 0 && errno == EINTR) {
				continue;
			}
			// 成功、EAGAIN（已经被清掉），或 shutdown 阶段的其他错误，都不必重试。
			return;
		}
	}

	/*
	 * stream_descriptor 必须拥有自己的 fd，不能直接 stream_descriptor sd(exec, event_fd_)，
	 * 否则 Doorbell 和 stream_descriptor 都会认为自己拥有同一个 fd。
	 */
	[[nodiscard]] int duplicate_event_fd() const noexcept {
		return ::fcntl(event_fd_, F_DUPFD_CLOEXEC, 0);
	}

	[[nodiscard]] bool is_rung() const noexcept {
		return rung_.load(std::memory_order_relaxed);
	}

private:
	// 通过fd唤醒消费者线程来读取队列
	void notify() const noexcept {
		constexpr uint64_t one = 1;
		while (true) {
			// 给eventfd计数器+1
			const ssize_t n = ::write(event_fd_, &one, sizeof(one));
			if (n < 0 && errno == EINTR) {
				continue;
			}
			/*
			 * EAGAIN 意味着 eventfd counter 已经非常大，fd 本身已经可读，不用继续写。
			 * 其他错误通常只会发生在 shutdown / 生命周期错误阶段。
			 */
			return;
		}
	}

	// 生产者读写、消费者读写；fd 只读，和标志放在同一缓存行，敲门的生产者拿到行后顺带读到。
	std::atomic<bool> rung_ {false};
	int event_fd_ {-1};
};

// MPSC 队列；唤醒由分片共用的 Doorbell 负责，push 不写 eventfd。
template <typename Message>
class alignas(64) Mailbox {
	static_assert(std::is_trivially_copyable_v<ShardEvent<Message>>, "ShardEvent must be trivially copyable");
	using MailboxQueue = boost::lockfree::mpsc_weak_queue<ShardEvent<Message>>;

public:
	Mailbox() : queue_(kMailboxCapacity) {
	}

	~Mailbox() {
		// 释放残留消息，避免 shutdown 时泄漏。
		ShardEvent<Message> ev {};
		while (queue_.pop(ev)) {
			delete ev.message;
			ev.message = nullptr;
		}
	}

	Mailbox(const Mailbox &) = delete;
	Mailbox &operator=(const Mailbox &) = delete;

	// 成功时 queue 取得所有权。调用方负责在之后敲目标分片的门铃。
	bool try_push(std::unique_ptr<Message> &message) {
		if (!message) {
			return false;
		}
		ShardEvent<Message> ev {};
		ev.message = message.get();
		if (!queue_.push(ev)) {
			return false;
		}
		// queue 已经拥有 message。释放对message的所有权 防止delete
		message.release();
		return true;
	}

	// consumer 取得 ownership。
	bool pop(std::unique_ptr<Message> &out) {
		ShardEvent<Message> ev {};
		if (!queue_.pop(ev)) {
			return false;
		}
		out.reset(ev.message);
		return true;
	}

private:
	MailboxQueue queue_;
};
} // namespace fastest
