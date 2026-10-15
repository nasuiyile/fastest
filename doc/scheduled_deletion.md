# 定时过期策略
（暂未实现）
过期策略：惰性过期 + 每个 shard 内有预算的、自适应增量扫描。
LRU，LFU如果达到了上限，那么直接由本次写入这个key的协程负责清理然后再返回给用户。
考虑到时间轮算法的额外内存开销。因此采用协作式调度+定期采样。

实现优先级调度，接收TCP请求，处理数据，写回TCP响应的线程，共三种为第一优先级，在后台进行定期抽样的为第二优先级。优先运行第一优先级的任务。
过期行为与memcached保持一致接受uint32的时间戳，保存秒数，0表示永不过期，最大支持30天，超过30天的数值会被当做unix时间戳来处理。

当数据库没有任何TTL key 的时候，跳过主动过期扫描。所有带过期时间的，不带过期时间的都放到一个hashmap中。

当进行过期操作的时候，每50ms一轮，每次最多运行1ms。

遍历数量T=100，每次采样T个，取里面需要过期时间的，假设为20个，D=20。如果20个中25%以上都是需要过期的，那么就再执行一次删除算法。
每次采样的时候维护一个游标，记录最后采样过的位置。每次从之前最后的位置开始采样。
每次执行完一次，要进入下一次的时候，查看是否达到了50ms，或者其他线程有没有到来。如果没有到来，并且阈值达标，那么就继续一次后台删除。

优先级调度代码
~~~C++
void run() {
    asio::co_spawn(io_, worker_->mailbox_request_loop(), asio::detached);
    asio::co_spawn(io_, worker_->mailbox_response_loop(), asio::detached);
    asio::co_spawn(io_, accept_loop(), asio::detached);
    while (!stop_->load(std::memory_order_relaxed) && !io_.stopped()) {
        // 前台优先。
        if (io_.poll() != 0)
            continue;
    
        // poll() 也可能因为 context 停止而返回 0。
        if (stop_->load(std::memory_order_relaxed) || io_.stopped())
            break;
    
        // 每次清理返回后，重新检查退出条件和前台任务。
        if (clean_up_once()) //clean_up_once()
            continue;
    
        // 接受空闲时最多隔 500ms 重新检查清理。
        io_.run_one_until(std::chrono::steady_clock::now() + 500ms);
    }
}
~~~
因为存在惰性删除，在访问频繁的情况下，数据也会自动被删除，暂时不考虑后台清理线程饿死的问题。