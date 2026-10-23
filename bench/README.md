# fastest vs Garnet 跑分

用同一个压测客户端 [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark) 分别压 fastest（memcached 文本协议）
和 [Garnet](https://github.com/microsoft/garnet)（RESP），比较 GET/SET 混合负载下的吞吐、延迟、CPU 和内存。

```bash
BENCH_HOST=192.168.31.29 BENCH_PASS=<root 密码> bench/bench.sh
```

开发机上执行，依次完成：

1. 按 Release 编译 fastest（`cmake-build-bench/`，vcpkg 工具链默认 `~/vcpkg`，可用 `VCPKG_ROOT` 指定）；
2. 把二进制和 `remote/*.sh` 传到压测机的 `/root/fastest-bench`；
3. `remote/setup.sh`：编译 memtier、安装 .NET 运行时、下载 Garnet 发布包，全部放在 `/root/fastest-bench` 下，可重复执行；
4. `remote/run.sh fastest`、`remote/run.sh garnet`：两个服务端先后跑同一套矩阵，每种 value 大小约 40 秒，各约 2 分钟；
5. 取回到 `bench/results/<时间>/`，`report.py` 生成 `report.md` 和 `summary.csv`。

只想重跑其中一个：`SERVERS=garnet SKIP_BUILD=1 bench/bench.sh`。

## 测试方法

- **裸机、绑核**：服务端和 memtier 都直接跑在压测机上（不用容器），用 `taskset` 分开：
  服务端 `0-7`（9950X CCD0 的 8 个物理核，不用 SMT 兄弟线程），memtier `8-15,24-31`（CCD1，16 线程）。
  fastest 的分片数取进程 CPU 亲和性里的 CPU 数（`util::core_num()`），所以同样是 8 个分片；Garnet 的 .NET 线程池也按亲和性取 8。
- **纯内存**：fastest 本身没有持久化；Garnet 显式关闭 AOF、分层存储（`--storage-tier false`）、恢复、定时 checkpoint 和 compaction，
  `--memory 32g` 保证数据全部在内存日志里，不会被挤出。
- **矩阵**：value 大小 `64B / 1KB / 8KB` × 流水线深度 `1 / 16 / 64` × 连接数 `16 / 512`，共 18 个点，每点 5 秒。
  128 连接的结果夹在两端之间（流水线 1 时与 512 几乎相同），不再单测。
- **负载**：100 万个 key，SET:GET = 1:10（memtier 默认比例），key 均匀随机。每种 value 大小重启一次服务端，
  先按顺序写满全部 key（之后 GET 全部命中），再预热 2 秒（让 Garnet 的 JIT 分层编译走完），然后逐点测量。
- **指标**：吞吐、p50/p99/p99.9 延迟（memtier 统计），稳态区间内服务端和客户端 CPU 组的平均忙碌率（`/proc/stat`），
  预填充后服务端 RSS。客户端 CPU 接近 100% 时说明瓶颈在压测端，该点的比较没有意义。

所有参数都可以用环境变量覆盖（见 `remote/run.sh` 开头），例如：

```bash
SIZES="32 4096" PIPELINES="1 32" CONNS="64" TEST_TIME=10 BENCH_HOST=... bench/bench.sh
```

## 改动前后对比（A/B）

`remote/ab.sh` 在压测机上对比改动前后的两个 fastest 二进制：对每个 value 大小（一项），`new`（`NEW_BIN NEW_ARGS`）
和 `base`（`BASE_BIN BASE_ARGS`）各自启动、预填充，跑同一组测试点；每轮轮换两者的先后顺序，取各轮中位数。
默认纯 GET、2 轮、测试点 `POINTS="1:128 16:128"`（`<流水线>:<连接数>`）、每点 4 秒，一项约 45 秒。

```bash
# 开发机：两份源码各自按 Release 编译，传到压测机
scp <新版本>/fastest root@$BENCH_HOST:/root/fastest-bench/fastest-new
scp <改前版本>/fastest root@$BENCH_HOST:/root/fastest-bench/fastest-base
scp bench/remote/ab.sh root@$BENCH_HOST:/root/fastest-bench/
# 压测机
bash ab.sh results/ab                                                         # 均匀访问，64B ~ 16KB，约 4 分钟
KEYS=16 TAG=hot bash ab.sh results/ab                                         # 16 个热点 key
TAG=mix RATIO=1:10 SIZES="64 1024 8192" POINTS="1:512 16:128" bash ab.sh results/ab   # SET:GET = 1:10
SERVER_CPUS=0-15 CLIENT_CPUS=16-31 bash ab.sh results/ab                      # 服务端跨两个 CCD（客户端用 SMT 兄弟线程）
# 开发机
python3 bench/ab_report.py bench/results/ab/sweep.csv
```

`results/share-*`、`results/fast-*`、`results/ccd-*` 是「小 value 跨分片拷贝」实验的结果（结论见 doc/core_design.md，已回退），
其中的 copy / share 模式是当时实验版本的 `--share-min-bytes` 开关。
