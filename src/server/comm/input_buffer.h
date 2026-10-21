#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>

namespace fastest {

// 连接的输入缓冲：socket 直接读进尾部空闲区（没有中转 buffer），消费只移动起点；
// 只在尾部空间不够时才把剩下的半条命令搬到开头或扩容。清空后若容量过大则释放，空闲连接不长期占内存。
class InputBuffer {
public:
	static constexpr std::size_t kRetainBytes = 64 * 1024;

	InputBuffer() = default;

	InputBuffer(const InputBuffer &) = delete;

	InputBuffer &operator=(const InputBuffer &) = delete;

	[[nodiscard]] std::span<const char> readable() const noexcept {
		return {data_.get() + begin_, end_ - begin_};
	}

	[[nodiscard]] std::size_t size() const noexcept {
		return end_ - begin_;
	}

	[[nodiscard]] bool empty() const noexcept {
		return begin_ == end_;
	}

	[[nodiscard]] std::size_t capacity() const noexcept {
		return capacity_;
	}

	// 返回至少 min_bytes 的可写尾部；读完后用 commit() 提交实际写入的字节数。
	[[nodiscard]] std::span<char> prepare(const std::size_t min_bytes) {
		if (capacity_ - end_ < min_bytes) {
			const std::size_t used = size();
			if (capacity_ - used >= min_bytes) {
				// 只搬未消费的部分（通常是半条命令）。
				if (used != 0) {
					std::memmove(data_.get(), data_.get() + begin_, used);
				}
			} else {
				if (min_bytes > max_size() - used) {
					throw std::length_error("InputBuffer capacity overflow");
				}
				const std::size_t wanted = used + min_bytes;
				const std::size_t grown = capacity_ > max_size() / 2 ? max_size() : capacity_ * 2;
				const std::size_t capacity = std::max(wanted, grown);
				auto data = std::make_unique_for_overwrite<char[]>(capacity);
				if (used != 0) {
					std::memcpy(data.get(), data_.get() + begin_, used);
				}
				data_ = std::move(data);
				capacity_ = capacity;
			}
			begin_ = 0;
			end_ = used;
		}
		return {data_.get() + end_, capacity_ - end_};
	}

	void commit(const std::size_t bytes) noexcept {
		end_ += bytes;
	}

	void consume(const std::size_t bytes) noexcept {
		begin_ += bytes;
		if (begin_ == end_) {
			begin_ = 0;
			end_ = 0;
			if (capacity_ > kRetainBytes) {
				data_.reset();
				capacity_ = 0;
			}
		}
	}

private:
	static constexpr std::size_t max_size() noexcept {
		return static_cast<std::size_t>(-1) / 2;
	}

	std::unique_ptr<char[]> data_;
	std::size_t capacity_ = 0;
	std::size_t begin_ = 0;
	std::size_t end_ = 0;
};

} // namespace fastest
