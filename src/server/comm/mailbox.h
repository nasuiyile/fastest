#pragma once
#include <boost/asio/detail/config.hpp> // BOOST_ASIO_HAS_IO_URING_AS_DEFAULT

#include <sys/eventfd.h>
#include <atomic>
#include <cerrno>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <system_error>
#include <unistd.h>

namespace fastest {
// 相邻行预取器按 128B 成对拉取缓存行，门铃标志独占一对，避免和队列的 head/tail 伪共享。
inline constexpr std::size_t kCacheLinePair = 128;

/*
 * 门铃：每个分片一个，配合分片的收件队列（MpscQueue）使用（一个 eventfd + 一个标志）。
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
 * Vyukov 队列的 push 分两步（交换尾指针 → 链接前驱），另一个生产者卡在两步之间时 pop 会提前返回空，
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

// 侵入式 MPSC 队列的链接钩子：节点类型继承它，队列只操作钩子，不分配内存。
struct MpscHook {
	std::atomic<MpscHook *> next {nullptr};
};

/*
 * 侵入式 Vyukov MPSC 队列：多个生产者，一个消费者（队列所属的分片线程）。
 * push = 一次 exchange 加一次 release store，没有分配、没有 freelist、没有 CAS 重试；唤醒由分片的 Doorbell 负责。
 * 生产者竞争的 tail_ 和消费者独占的 head_ 各占一对缓存行。
 * push 分两步（交换尾指针 → 链接前驱）：另一个生产者卡在两步之间时 pop 会提前返回 nullptr，排在它后面的节点暂时取不到，
 * 卡住的生产者链接完成后照常敲门，所以不会丢（见 Doorbell）。
 * pop 返回的节点之后不会再被生产者写 next，消费者可以立刻把它推进别的队列。
 * 析构时 delete 残留节点（只在 shutdown 时发生）。
 */
template <typename Node>
    requires std::derived_from<Node, MpscHook>
class MpscQueue {
public:
	MpscQueue() noexcept : tail_(&stub_), head_(&stub_) {
	}

	~MpscQueue() {
		while (Node *node = pop()) {
			delete node;
		}
	}

	MpscQueue(const MpscQueue &) = delete;
	MpscQueue &operator=(const MpscQueue &) = delete;

	// 任意线程；调用方负责之后敲目标分片的门铃。
	void push(Node *node) noexcept {
		link(node);
	}

	/*
	 * 不打算敲门的生产者用（只带归还记录、不急着处理的信封）：链接完成后做一次 seq_cst fence，
	 * 返回此时是否已经有别的节点排在它后面。
	 *
	 * 为什么要看后面：push 分两步，本节点「交换了尾指针、还没链接」期间，排在后面的生产者 P 即使链接好、敲了门，
	 * 消费者 pop 到本节点的前驱就停下（返回空），放下门铃、复查仍为空就睡了，P 的那次敲门已经被这次醒来用掉。
	 * 普通生产者链接完成后都会再检查一次门铃，由它把消费者叫醒；不敲门的生产者也必须接住这个责任：
	 * 后面有节点、并且门铃是放下的，就要敲门。后面没有节点时，队列里卡住的只有自己，不需要叫醒任何人。
	 * 调用方随后读门铃（knock() 要求的 fence 已经在这里做过）。
	 * 读的是队列自己的尾指针，不读节点：节点链接之后随时可能被消费者取走、复用甚至释放。
	 */
	[[nodiscard]] bool push_quiet(Node *node) noexcept {
		link(node);
		return queued_behind(node);
	}

#if defined(FASTEST_TESTING)
	// 只给测试用：把 push_quiet 拆成「交换尾指针」和「链接前驱 + 检查」两步，构造生产者卡在两步之间的情形。
	MpscHook *begin_push_for_test(Node *node) noexcept {
		node->next.store(nullptr, std::memory_order_relaxed);
		return tail_.exchange(node, std::memory_order_acq_rel);
	}

	[[nodiscard]] bool finish_push_quiet_for_test(MpscHook *prev, Node *node) noexcept {
		prev->next.store(node, std::memory_order_release);
		return queued_behind(node);
	}
#endif

	// 只有消费者调用。队列为空，或者唯一的后继还没被生产者链接上时，返回 nullptr。
	Node *pop() noexcept {
		MpscHook *head = head_;
		MpscHook *next = head->next.load(std::memory_order_acquire);
		if (head == &stub_) {
			if (next == nullptr) {
				return nullptr;
			}
			head_ = next;
			head = next;
			next = next->next.load(std::memory_order_acquire);
		}
		if (next != nullptr) {
			head_ = next;
			return static_cast<Node *>(head);
		}
		if (head != tail_.load(std::memory_order_acquire)) {
			return nullptr; // 有生产者交换了尾指针、还没链接
		}
		// head 是最后一个节点：把 stub 挂到它后面，才能把它取走。
		link(&stub_);
		next = head->next.load(std::memory_order_acquire);
		if (next != nullptr) {
			head_ = next;
			return static_cast<Node *>(head);
		}
		return nullptr;
	}

private:
	[[nodiscard]] bool queued_behind(const Node *node) const noexcept {
		std::atomic_thread_fence(std::memory_order_seq_cst);
		return tail_.load(std::memory_order_relaxed) != node;
	}

	void link(MpscHook *node) noexcept {
		node->next.store(nullptr, std::memory_order_relaxed);
		MpscHook *prev = tail_.exchange(node, std::memory_order_acq_rel);
		prev->next.store(node, std::memory_order_release);
	}

	alignas(kCacheLinePair) std::atomic<MpscHook *> tail_; // 生产者 exchange
	alignas(kCacheLinePair) MpscHook *head_;               // 只有消费者读写
	MpscHook stub_;                                        // 队列空时的占位节点
};
} // namespace fastest
