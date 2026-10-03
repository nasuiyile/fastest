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
#include <string>

// 事件只持有指向消息的指针，因此与消息类型无关存储布局
template <typename Message>
struct ShardEvent {
	Message *message{nullptr};
};

// constexpr std::size_t kMailboxCapacity = 4096;

namespace comm {
template <typename Message>
class alignas(64) Mailbox {
	static_assert(std::is_trivially_copyable_v<ShardEvent<Message> >, "ShardEvent must be trivially copyable");
	// using MailboxQueue = boost::lockfree::mpsc_weak_queue
	// <ShardEvent<Message>, boost::lockfree::capacity<kMailboxCapacity> >;
	using MailboxQueue = boost::lockfree::mpsc_weak_queue<ShardEvent<Message> >;

public:
	Mailbox() : queue_(1024){
		event_fd_ = ::eventfd(0,EFD_NONBLOCK | EFD_CLOEXEC);
		if (event_fd_ < 0) {
			throw std::system_error(errno, std::generic_category(), "eventfd");
		}
	}

	~Mailbox() {
		/*
		 * 这里仍然把残留消息释放掉，避免 shutdown 时泄漏。
		 */
		ShardEvent<Message> ev{};
		while (queue_.pop(ev)) {
			delete ev.message;
			ev.message = nullptr;
		}
		if (event_fd_ >= 0) {
			::close(event_fd_);
			event_fd_ = -1;
		}
	}


	// 禁止拷贝
	Mailbox(const Mailbox &) = delete;

	Mailbox &operator=(const Mailbox &) = delete;

	bool try_push(std::unique_ptr<Message> &message) {
		if (!message) {
			return false;
		}
		ShardEvent<Message> ev{};
		ev.message = message.get();
		if (!queue_.push(ev)) {
			return false;
		}
		// queue 已经拥有 message。释放对message的所有权 防止delete
		message.release();
		//确保notify是在push完成后才发生，不会撞上半完成的pusd
		notify();
		return true;
	}


	/*
	 * consumer 取得 ownership。
	 */
	bool pop(std::unique_ptr<Message> &out) {
		ShardEvent<Message> ev{};
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
		return ::fcntl(event_fd_,F_DUPFD_CLOEXEC, 0);
#else
		const int fd = ::dup(event_fd_);
		if (fd >= 0) {
			const int flags = ::fcntl(fd, F_GETFD);
			if (flags >= 0) {
				::fcntl(fd,F_SETFD,flags | FD_CLOEXEC);
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
			// errno自动保存最近一次系统调用失败的原因
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

	MailboxQueue queue_;
	int event_fd_{-1};
};
} // namespace comm