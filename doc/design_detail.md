#设计细节

## 响应发送

读和写互不等待。session 协程只负责读、解析和执行；响应进入连接的环形数组后只记下连接，批次结束时（解析批次或收件协程的一轮，
见 core_design.md「批次作用域：合并敲门和写」）每个连接调用一次 `kick_write`，不等待 socket。
回程信封里的跨分片响应也一样只入环，因此一个不读响应的慢连接不会拖住同分片的其他连接。

每个连接同一时刻最多一个 `async_write` 在途（`TCPSession::writing`）。`kick_write` 按序号在环形数组里原地编码所有就绪的响应（不搬出槽位，只推进发送游标），
用 `GatherSink` 同步编码成 `const_buffer` 列表，一次 `async_write`（writev）发出：协议文本、数字和不超过 256 字节的字段
拷进连续的 arena 并合并成一段，更长的 value 只引用不复制。写在途期间新就绪的响应留在环形数组，由写完成回调作为下一批发出，
流水线请求因此自然合并成少量系统调用。每批最多 1MB 或 256 条响应。

环形数组兼作在途缓冲，有两个游标：`[head, next_id)` 已编码进在途写，写完成后 `release_sent()` 一次释放；
`next_id` 之后等待发送。容量从 `head` 算起，扩容会移动在途槽位：这依赖 `GatherSink` 只引用槽位之外的内存
（`CacheValue` 的 payload），key 和短字段都拷进 arena。多分片响应不合并，编码时按到达顺序依次输出各分片的条目，最后一个 `END`。

编码在写之前完成：某条响应字段非法时，撤销它已追加的部分，改发 `SERVER_ERROR` 占住这个序号，不会发出半个响应。

背压：已分配序号、但还没交给写操作的请求数（`TCPSession::backlog()`）达到 2048 时，session 协程暂停解析和读取，
等写回调把它降到 256 以下再恢复。丢弃过大数据块的路径不受影响，否则 swallow 永远无法完成。

回收：写在途时不能回收 session，asio 和内核仍引用环形数组中已发送的槽位。session 协程退出或连接关闭时如果仍有写在途，
由写完成回调回收（`reclaim_session_if_idle`），取消的写同样会回调。

## Value 的生命周期

value 字节从 socket 到存储只拷贝一次，之后只传递 `CacheValuePtr`（侵入式 u32 引用计数，普通整数，发布后只有 owner 读写）和
借出凭证 `ValueLoan`（见 core_design.md「借出与归还」）。

- 解析：数据块完整到达时，解析器把它直接拷进 `CacheValue::create()` 分配的最终存储块（一次分配：
  `[CacheValue 24B][payload][\r\n]`，flags 写在块头）。存储命令只持有 `CacheValuePtr`。
- 头部字段：命令不重复保存 value 头里已有的字段，解析时就写成最终值。
  - `flags` 原样写入。
  - `exptime` 按 `util::ExpiryClock` 换算成绝对的 `expired_at`：相对纪元的秒数，0 永不过期，1 已过期；不超过 30 天按相对秒数，
    更大的按 unix 时间戳，早于纪元视为已过期。一批命令最多读一次时钟（coarse 单调时钟），exptime 为 0 时不读。
    exptime 与 memcached 一样是 int32：按 `safe_strtol` 解析后截断成 int32，负数立即过期（所以 4294967295 即 -1，立即过期）。
  - cas 命令的 `cas_unique`（客户端期望的旧版本）暂存在尚未发布的 `value->cas`。
  - exptime、cas unique、incr/decr 的 delta 以及 value 里的计数器都用 `src/protocol/memcached_number.h`
    （`safe_strtoull` / `safe_strtol` 的等价实现，只在栈上计算），前导空白、`+`、尾部空白等边界与 memcached 一致。
  - gat/gats/touch 不带 value，过期时间留在命令里，但同样在解析时换算成 `expired_at`。
- 大数据块：命令头已完整、数据块还差至少 `ParserLimits::direct_read_min_bytes`（默认 4096）字节时，解析器先按声明长度分配
  value，拷入已到达的部分，返回 `PendingValue`。会话用 `async_read` 把剩余字节（含结尾 CRLF）直接读进 value，再由
  `MemcachedParser::complete()` 校验 CRLF（失败返回可恢复的 `bad data chunk`）。大 value 不经过、也不撑大连接的输入缓冲区。
- 输入缓冲：`InputBuffer` 让 socket 直接读进尾部空闲区，消费只移动起点，没有中转数组，也没有每批 `erase` 的 memmove；读空后容量超过
  64KB 就释放。
- 存储：覆盖写入都经过 `ShardCache::publish()`。它要求 value 尚未发布（引用计数为 1），把 `value->cas` 改写成新版本号
  （每分片自增，低 16 位是分片号，永不为 0；cas 命令先用期望值比较，再改写），然后插入。存进 map 的 value 字段全部是最终值。
  add 用 `ExpiredMap::insert()`：先直接插入（key 不存在时只查找一次），已存在时再判断是否过期，插入成功后才分配版本号。
  replace/append/prepend/incr/decr 对过期的 key 按不存在处理。append/prepend 分配合并后的新 value，沿用旧值的 flags 和
  `expired_at`（命令里的 flags、exptime 被忽略），合并后超过 `ParserLimits::max_value_bytes` 时 NOT_STORED。
  `ShardCache` 使用 `ExpiredMap`（`src/cache/expired_map.h`，内部是 `IncrementalHashMap<CacheValue>`），map 中每个指针持有一次计数。
  插入成功后 `release_ownership()` 交给 map，覆盖、删除、过期、析构时 `release()`。
- 过期：判断条件为 `expired_at != 0 && expired_at <= ExpiryClock::now()`，`expired_at` 为 0 时不读时钟。发现过期就当场删除并释放，
  不返回给调用者：get/gets/cas 的查找、delete 都会惰性删除过期的 key（cas 返回 NOT_FOUND，delete 返回 NOT_FOUND）；
  `ShardCache::sample_expired(limit)` 供外部调度做主动过期，内部调用 `IncrementalHashMap::sample_next` 的三参数版本，
  回调判断过期后返回 true，由 map 当场摘下节点并释放，返回本次采样数、带 TTL 数、过期删除数。
- TTL 计数：`ExpiredMap` 用一个普通 `size_t`（分片线程私有，不用 atomic，不增加每个 pair 的内存）记录带过期时间的条目数。
  插入带过期时间的 value 加一；这样的条目被覆盖、delete、惰性删除、采样删除时减一。计数为 0 时 `sample_expired` 直接返回，
  不扫描、不读时钟。touch/gat 原地修改 `expired_at` 都经过 `ExpiredMap::touch()`，由它同步调整计数。
- 读取：命中时 `ValueLoan::lend()` 加一次计数放进响应，key 从命令移动进响应。value 发布后只读，跨线程共享安全；之后的覆盖或删除只释放
  map 那一次计数，在途响应继续引用旧值直到写完成、归还记录送回 owner。gat/gats 命中时先改 `expired_at` 再返回（不改 cas）；远端在途响应不读 `expired_at`。
- incr/decr：结果与 memcached `do_add_delta` 逐字节相同。新数字不比旧数据长时长度不变、尾部补空格，变长时刚好装下数字；
  flags、`expired_at` 不变，cas 换新。value 只被 map 持有时（`CacheValue::exclusively_owned()`，owner 读自己的计数，相当于
  memcached 的 refcount == 2）原地改写，不分配；有在途响应引用（或者归还记录还没送到）时分配新 value，同样补空格，让结果不取决于
  同一连接流水线里前一条回复何时写完、归还何时送到（memcached 每条 get 的回复写完才执行下一条命令）。这是“发布后只读”的唯一例外。
- 发送：编码直接输出 `view_crlf()`。超过 256 字节的 value 作为独立 iovec 直接引用 CacheValue；不超过 256 字节的拷进发送
  arena 与协议文本合并，用一次小拷贝换更少的 iovec 和系统调用。
- 多 key：key 按分片移动拆分；全部落在同一分片时整条命令原样转交，不拆分。

分片选择 `util::get_shard` 使用 hash 高 32 位（multiply-shift，无除法）；分片内的 map 用低位寻址 2 的幂个桶，两者互不相关，避免分片数为
2 的幂时同一分片只用到部分桶。
