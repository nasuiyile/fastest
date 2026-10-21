#设计细节

## 响应发送

读和写互不等待。session 协程只负责读、解析和执行；响应进入连接的环形数组后调用 `kick_write`，不等待 socket。
mailbox 收到跨分片响应也一样只入队，因此一个不读响应的慢连接不会拖住同分片的其他连接。

每个连接同一时刻最多一个 `async_write` 在途（`TCPSession::writing`）。`kick_write` 按序号取出环形数组中所有就绪的响应，
用 `GatherSink` 同步编码成 `const_buffer` 列表，一次 `async_write`（writev）发出：协议文本、数字和不超过 256 字节的字段
拷进连续的 arena 并合并成一段，更长的 value 只引用不复制。写在途期间新就绪的响应留在环形数组，由写完成回调作为下一批发出，
流水线请求因此自然合并成少量系统调用。每批最多 1MB 或 256 条响应。

编码在写之前完成：某条响应字段非法时，撤销它已追加的部分，改发 `SERVER_ERROR` 占住这个序号，不会发出半个响应。

背压：已分配序号、但还没交给写操作的请求数（`TCPSession::backlog()`）达到 1024 时，session 协程暂停解析和读取，
等写回调把它降到 256 以下再恢复。丢弃过大数据块的路径不受影响，否则 swallow 永远无法完成。

回收：写在途时不能回收 session，asio 和内核仍引用 `inflight` 中的响应。session 协程退出或连接关闭时如果仍有写在途，
由写完成回调回收（`reclaim_session_if_idle`），取消的写同样会回调。

## Value 的生命周期

value 字节从 socket 到存储只拷贝一次，之后只传递 `CacheValuePtr`（侵入式 u32 引用计数）。

- 解析：数据块完整到达时，解析器把它直接拷进 `CacheValue::create()` 分配的最终存储块（一次分配：
  `[CacheValue 24B][payload][\r\n]`，flags 写在块头）。存储命令只持有 `CacheValuePtr`。
- 头部字段：命令不重复保存 value 头里已有的字段，解析时就写成最终值。
  - `flags` 原样写入。
  - `exptime` 按 `util::ExpiryClock` 换算成绝对的 `expired_at`：相对纪元的秒数，0 永不过期，1 已过期；不超过 30 天按相对秒数，
    更大的按 unix 时间戳，早于纪元视为已过期。一批命令最多读一次时钟（coarse 单调时钟），exptime 为 0 时不读。
  - cas 命令的 `cas_unique`（客户端期望的旧版本）暂存在尚未发布的 `value->cas`。
  - gat/gats/touch 不带 value，过期时间留在命令里，但同样在解析时换算成 `expired_at`。
- 大数据块：命令头已完整、数据块还差至少 `ParserLimits::direct_read_min_bytes`（默认 4096）字节时，解析器先按声明长度分配
  value，拷入已到达的部分，返回 `PendingValue`。会话用 `async_read` 把剩余字节（含结尾 CRLF）直接读进 value，再由
  `MemcachedParser::complete()` 校验 CRLF（失败返回可恢复的 `bad data chunk`）。大 value 不经过、也不撑大连接的输入缓冲区。
- 输入缓冲：`InputBuffer` 让 socket 直接读进尾部空闲区，消费只移动起点，没有中转数组，也没有每批 `erase` 的 memmove；读空后容量超过
  64KB 就释放。
- 存储：所有写入都经过 `ShardCache::publish()`。它要求 value 尚未发布（引用计数为 1），把 `value->cas` 改写成新版本号
  （每分片自增，低 16 位是分片号，永不为 0；cas 命令先用期望值比较，再改写），然后插入。存进 map 的 value 字段全部是最终值。
  `ShardCache` 使用 `IncrementalHashMap<CacheValue>`，map 中每个指针持有一次计数。插入成功后 `release_ownership()` 交给
  map，覆盖、删除、析构时 `release()`。
- 读取：命中时 `CacheValuePtr::share()` 加一次计数放进响应，key 从命令移动进响应。value 发布后只读，跨线程共享安全；之后的覆盖或删除只释放
  map 那一次计数，在途响应继续引用旧值直到写完成。
- 发送：编码直接输出 `view_crlf()`。超过 256 字节的 value 作为独立 iovec 直接引用 CacheValue；不超过 256 字节的拷进发送
  arena 与协议文本合并，用一次小拷贝换更少的 iovec 和系统调用。
- 多 key：key 按分片移动拆分；全部落在同一分片时整条命令原样转交，不拆分。

分片选择 `util::get_shard` 使用 hash 高 32 位（multiply-shift，无除法）；分片内的 map 用低位寻址 2 的幂个桶，两者互不相关，避免分片数为
2 的幂时同一分片只用到部分桶。
