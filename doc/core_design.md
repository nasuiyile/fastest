## 核心设计

兼容memcached文本协议，后续还兼容Meta协议。 每个网络线程均采取协作式调度，保留自己的Data HashMap。
如果收到请求的数据，在解析后能被当前线程直接处理，那么直接完成处理，如果不能处理则投递给其他的线程。
采取协作式调度保证了，每个请求处理线程在进行网络IO操作导致空闲的时候，也能处理其他线程来的请求。
整体的线程运行机制类似于Dragonflydb的shared-nothing架构。整体采用C++20中Boost。
网络采用io_uring+proactor模型。
与Dragonflydb不同的地方在于每个线程都持有俩个mailbox，一个是用于接收别人发给自己的请求，另一个是用来接收别人发给自己的响应。因此请求响应都通过mailbox完成。
这个mailbox是MPSC的，通过最新的库`<boost/lockfree/mpsc_weak_queue.hpp>`来实现，不使用SPSC是因为，如果要给每一个线程之间建立一个专用的通道，会导致NxN的通道数量，并且核心数多的情况下，轮询通道也会成为性能开销。
在memcached协议中，需要顺序来回复用户的请求，因此当收到响应消息，就把该响应写到环形数组中（环形数组只是用来保证顺序回复的），每个TCP的session维护一个环形数组。

发送端回通过fd来进行相互之间的唤醒和通信。先push，之后调用notify write (fd)来唤醒其他线程。对于TCP连接,每次启动都启动一条session协程,因此使用co_await 可以停在当前连接上。
然后开始消费队列。当调用pop方法返回false的时候，调用asio::posix::stream_descriptor descriptor(executor, wait_fd);的 async_wait进行等待。
async_wait → clear_notify → pop。

每次在给其他线程发送请求的时候，记录一个 origin_shard u16作为线程号，以便获取回复。i32作为tcp的id。
不同TCP的session是单独的，每个线程都维护一个u64来作为请求编号ID。
收到请求：

被唤醒后，直接榨干队列中的所有元素，重置fd，然后给这些元素进行对应的操作。由于收到的请求一定是属于当前分片的，可以直接写入对应的Data hashmap
然后push到调用者的回复队列中。然后唤醒对应的线程。


收到回复：
直接通过tcp的32位fd写入回复。
环形数组中数组记录了下一个需要回复给用户消息的ID。另外每个回复的ID对数组长度求余得到写入的位置。
如果 `当前回信的ID-下一个发给用户消息>=数组的长度`，那么说明环形数组满了，触发一次扩容操作。默认长度为128，扩容后更新数组长度.按每个槽保存的完整序号迁移到 seq % new_capacity。
如果收到5号槽信息，但是1号槽数据还没来，那么就继续消费队列。
如果当前回信的ID等于下一个需要给用户回复消息的ID，那么就回复消息，然后检查下一个数组的位置是否为空，如果不为空就将下一个消息也发送给用户，然后将当前回信ID自增1 。
直到下一个ID为空，那么就继续消费。在发送给用户结束之后,清空已经发送的元素.

## 多分片操作
然而memcached中的命令，如get，gas是允许跨分片操作的。意味着有可能分发给多个分片多个操作的情况。因此现在环形数组收到的请求被分为了俩种，一个是多key操作的命令，一个是单key操作的命令。get等操作在第一次解析的时候就会被确定是单key还是多key。
因此将消息投递给其他线程的时候，以及回复的时候也会区分多key还是单key。如果是多key操作，会统计需要收集多少个分片的数据才能返回给客户端。因此收到回复时，会先放入到环形数组中，如果是多分片操作，那么需要等待所有分片操作全部完成，才能标记为当前数据齐全（检查就绪数组中元素的数量）。

## 补充
`<boost/lockfree/mpsc_weak_queue.hpp>` 是Vyukov MPSC，允许多个线程写入数据，只有一个线程可以读取。 读写都是无锁的。即便这个队列被设置为无界的，在没有发生扩容时，依然是无锁的（有界版本性能稍有优势）。

解析时，先将当前buffer中已经到达的多条命令先解析，让这些命令自然的形成批次。

后续实现doorbell suppression / coalescing机制。
给每个mailbox添加一个`atomic<bool>`，表示已经有人敲过门了。生产者
~~~C++
if (!notify_pending_.exchange(true)) {
    notify();
}
~~~
exchange(true) 把标志设为 true，同时返回旧值：
- 旧值为 false：我是第一个敲门的，调用 notify()。
- 旧值为 true：已经有人敲过了，直接跳过通知。

消费者醒来后按这个顺序处理：
~~~C++
clear_notify();                  // 清掉 eventfd 通知
notify_pending_.exchange(false); // 允许下一轮敲门
while (pop(message)) {
    handle(message);
}
~~~
从第一个生产者敲门，到消费者开始一轮消费之前。这期间线程可能在休眠，也可能已经醒了、正在处理其他socket操作。减少了syscall。

## 解析错误与连接恢复

解析器通过 `ParseBatchResult` 返回成功命令前缀、已消费字节数和可选错误。协议错误不抛异常。
`Handler` 先执行成功前缀，再给错误分配一个请求序号；错误与跨分片响应共用连接的 `RingArray`，不能越过前面的响应直接写 socket。
可恢复错误后继续解析缓冲区，批次不具有事务回滚语义。`noreply` 不输出字节，但仍完成该请求序号。

经典文本协议的恢复边界按 memcached 1.6.24 核对：

- 未知命令和错误的参数个数返回 `ERROR`，消费当前命令行并继续。
- 存储命令头的 key/数字格式错误返回 `CLIENT_ERROR bad command line format`，只消费命令头；客户端已经发送的 value 会被当作后续命令解析。
- 数据块末尾不是 CRLF 时，消费声明长度加两个字节，返回 `CLIENT_ERROR bad data chunk` 后继续。
- 长度合法但对象过大时，返回 `SERVER_ERROR object too large for cache`。会话按 `discard_bytes` 流式丢弃 value 和两个后缀字节，不把过大对象积攒在内存中；丢弃完成前暂停该错误序号的发送。
- 过大的 `set` 还必须删除同名旧值。`RejectedSetRequest` 将删除与错误一起路由到 key 所属分片，保证前面的正常 `set`、拒绝操作和后面的 `get` 按该分片的队列顺序执行。删除在吞包完成前生效。

配置限制等致命解析错误会停止读取新命令，先让之前的响应和错误按序发出，再关闭连接。
读协程退出时，若致命错误仍在等待跨分片结果，不立即删除会话；有序发送结束后由 Worker 回收。
网络断开或发送失败仍可能使客户端收不到已经执行的命令结果。

## 正常路径的开销与验证

跨分片命令不得引用连接的可变输入缓冲区。解析器只复制一次 value，随后通过命令、mailbox 和缓存移动其所有权。
批处理遍历命令引用，本地响应直接进入有序响应路径，不额外创建 mailbox 消息或错误处理协程。
多 key 的本地部分只执行一次。命令行通过 `memchr` 定位 LF；批次在首条完整命令后估算容量，减少容器扩容，空输入和未完成命令不预分配。
这些措施针对解析和路由开销；当前同步 socket 发送方式仍需要另外进行整机吞吐和慢客户端压测。

独立回归测试不依赖外部服务：

```sh
cmake --build cmake-build-debug -j2
ctest --test-dir cmake-build-debug -R '^fastest_protocol_tests$' --output-on-failure
python3 tests/python/test_protocol_errors.py --server cmake-build-debug/fastest
```

TCP 测试会启动并关闭自己的服务，端口 2333 必须空闲。可加 `--reference /path/to/memcached`，在参考实例上执行同一组普通错误、流水线和吞包场景。
原有 `fastest_tests` 是要求外部 echo 服务的旧测试，不属于这些协议回归测试。

当前兼容边界：缓存仅实现 set/get，其他命令、过期语义等尚未完整实现；保留原有命令行、key 数量及 value 大小限制，数值与 memcached 的配置不完全一致。
另外，memcached 1.6.24 的某些非法 GET 路径会清空待发响应。例如同一流水线中的 SET 后接超长 key 的 GET，SET 已执行，但待发的 STORED 会被丢弃。
当前实现保留成功前缀的响应，未复现这一特殊缓冲丢弃行为，所以这里的错误恢复对齐不代表完整协议或所有内部边界行为完全兼容。

参考：[memcached 1.6.24 文本协议实现](https://github.com/memcached/memcached/blob/1.6.24/proto_text.c)、[协议文档](https://github.com/memcached/memcached/blob/1.6.24/doc/protocol.txt)。
