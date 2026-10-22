
# 解析错误与连接恢复

解析器通过 `ParseBatchResult` 返回成功命令前缀、已消费字节数和可选错误。协议错误不抛异常。
`ErrorResponse::message` 是指向字面量的 `std::string_view`，错误响应不做堆分配。
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



带 key 的命令参数错误同样按 memcached 1.6.24 的检查顺序（参数个数 -> noreply -> key -> 数字）：

- touch/incr/decr 命令名后面必须是 2 到 3 个参数，delete 是 1 到 3 个，否则 `ERROR`；gat/gats 至少要有 exptime，只有 exptime 没有 key 时回复 `END`。
- noreply 只看最后一个参数：第三个参数不是 noreply 时被忽略；`touch k noreply`、`set k 0 0 noreply` 这类写法算 noreply，随后的格式错误不回复。
- touch/gat/gats 的 exptime 非法回复 `CLIENT_ERROR invalid exptime argument`，incr/decr 的 delta 非法回复 `CLIENT_ERROR invalid numeric delta argument`，
  delete 的可选参数只能是旧版的 `0` 和/或 noreply，否则回复 `CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]`。
- 数字按 `safe_strtol` / `safe_strtoull` 解析（`src/protocol/memcached_number.h`）：允许 `+`，exptime 截断成 int32，负数立即过期。

当前兼容边界：带 key 的命令（set/add/replace/append/prepend/cas/get/gets/gat/gats/delete/touch/incr/decr）都已实现，
`tests/python/test_command_semantics.py` 逐字节对照 memcached 1.6.24（加 `--reference` 时同一份用例也在 memcached 上跑一遍）；
不带 key 的命令（flush_all/stats/version/verbosity/quit）尚未实现。exptime 已在解析时换算并存进 value，惰性过期已实现，主动过期的采样接口（`ShardCache::sample_expired`）已提供，但还没有接入后台调度；保留原有命令行、key 数量及 value 大小限制，数值与 memcached 的配置不完全一致。
已知的其他差异：

- 多 key 的 get/gets/gat/gats 按分片到达顺序输出命中项（见 design_detail.md），memcached 按请求顺序；协议不要求顺序。
- 解析器先校验整行 key 再执行，gat/gats 中间出现超长 key 时一个都不 touch；memcached 边解析边执行，前面的 key 已经 touch。
- flags 和数据长度仍严格按无符号十进制解析：memcached 在 64 位上会接受 `+`，并把超过 32 位的 flags 截断（例如 4294967296 存成 0）。
- incr/decr 总是按原地替换的格式写回（不变长时补空格）；memcached 在其他连接正持有旧值时会改为分配不补空格的新 item，协议允许两者。
  超过 memcached 分块大小（约 512KB）的 value，memcached 一律按非数字处理，这里照常解析。
另外，memcached 1.6.24 的某些非法 GET 路径会清空待发响应。例如同一流水线中的 SET 后接超长 key 的 GET，SET 已执行，但待发的 STORED 会被丢弃。
当前实现保留成功前缀的响应，未复现这一特殊缓冲丢弃行为，所以这里的错误恢复对齐不代表完整协议或所有内部边界行为完全兼容。
