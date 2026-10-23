#!/usr/bin/env bash
# 在压测机上对一个服务端跑完整矩阵：value 大小 × 流水线深度 × 连接数。
# 用法: run.sh <fastest|garnet> <输出目录>
#
# 服务端和 memtier 都跑在裸机上，用 taskset 分到不同的 CCD，互不抢核。
# 每种 value 大小重启一次服务端：预填充全部 key → 预热（不计入）→ 依次测各个点。
# 结果：<输出目录>/<server>/*.json（memtier 原始输出）和 <输出目录>/points.csv（每个点一行）。
set -euo pipefail

SERVER=${1:?usage: run.sh <fastest|garnet> <out_dir>}
OUT=${2:?usage: run.sh <fastest|garnet> <out_dir>}

BENCH_DIR=${BENCH_DIR:-/root/fastest-bench}
SERVER_CPUS=${SERVER_CPUS:-0-7}         # 9950X：CCD0 的 8 个物理核（不用 SMT 兄弟线程 16-23）
CLIENT_CPUS=${CLIENT_CPUS:-8-15,24-31}  # CCD1 的 8 核 16 线程
CLIENT_THREADS=${CLIENT_THREADS:-16}
TEST_TIME=${TEST_TIME:-5}               # 每个点的秒数，至少 3（一种 value 大小约 40 秒）
WARMUP_TIME=${WARMUP_TIME:-2}
KEYS=${KEYS:-1000000}
RATIO=${RATIO:-1:10}                    # SET:GET
SIZES=${SIZES:-"64 1024 8192"}
PIPELINES=${PIPELINES:-"1 16 64"}
CONNS=${CONNS:-"16 512"}                # 128 连接的结果夹在 16 和 512 之间，流水线 1 时与 512 几乎相同，不再单测
GARNET_VERSION=${GARNET_VERSION:-2.2.4}
GARNET_MEMORY=${GARNET_MEMORY:-32g}

case $SERVER in
fastest) PORT=2333 PROTOCOL=memcache_text ;;
garnet) PORT=6380 PROTOCOL=redis ;;
*) echo "unknown server: $SERVER" >&2; exit 2 ;;
esac

MEMTIER=$BENCH_DIR/tools/memtier_benchmark
mkdir -p "$OUT/$SERVER"
OUT=$(cd "$OUT" && pwd)
ulimit -n 65536

for v in SERVER_CPUS CLIENT_CPUS CLIENT_THREADS TEST_TIME WARMUP_TIME KEYS RATIO SIZES PIPELINES CONNS; do
	echo "$v=${!v}"
done >"$OUT/$SERVER/params.txt"
if [[ $SERVER == garnet ]]; then
	printf 'GARNET_VERSION=%s\nGARNET_MEMORY=%s\n' "$GARNET_VERSION" "$GARNET_MEMORY" >>"$OUT/$SERVER/params.txt"
fi

port_listening() { ss -Hltn "sport = :$PORT" | grep -q .; }

SERVER_PID=
stop_server() {
	if [[ -n $SERVER_PID ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill "$SERVER_PID"
		wait "$SERVER_PID" 2>/dev/null || true
	fi
	SERVER_PID=
	# io_uring 持有的 socket 在进程退出后由内核异步释放，等监听端口真正关掉再启动下一个。
	for _ in $(seq 50); do
		port_listening || return 0
		sleep 0.1
	done
}
trap stop_server EXIT

start_server() {
	if port_listening; then
		echo "port $PORT is already in use" >&2
		exit 1
	fi
	local log=$OUT/$SERVER/server.log
	case $SERVER in
	fastest)
		taskset -c "$SERVER_CPUS" "$BENCH_DIR/fastest" >>"$log" 2>&1 &
		;;
	garnet)
		# 纯内存：不开 AOF、不分层到磁盘、不恢复、不做定时 checkpoint 和 compaction（这些本来也是默认值）。
		DOTNET_ROOT=$BENCH_DIR/dotnet taskset -c "$SERVER_CPUS" \
			"$BENCH_DIR/garnet/$GARNET_VERSION/GarnetServer" --port "$PORT" --bind 127.0.0.1 \
			--memory "$GARNET_MEMORY" --aof false --storage-tier false --recover false \
			--checkpoint-freq 0 --compaction-freq 0 >>"$log" 2>&1 &
		;;
	esac
	SERVER_PID=$!
	for _ in $(seq 100); do
		(exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
		kill -0 "$SERVER_PID" 2>/dev/null || break
		sleep 0.1
	done
	echo "$SERVER failed to start, see $log" >&2
	exit 1
}

memtier() {
	taskset -c "$CLIENT_CPUS" "$MEMTIER" -s 127.0.0.1 -p "$PORT" --protocol="$PROTOCOL" \
		--key-minimum=1 --key-maximum="$KEYS" --hide-histogram "$@"
}

# "0-3,8" -> "0 1 2 3 8"
expand_cpus() {
	local IFS=, part out=()
	for part in $1; do
		if [[ $part == *-* ]]; then out+=($(seq "${part%-*}" "${part#*-}")); else out+=("$part"); fi
	done
	echo "${out[@]}"
}
SERVER_CPU_LIST=$(expand_cpus "$SERVER_CPUS")
CLIENT_CPU_LIST=$(expand_cpus "$CLIENT_CPUS")

# 两次 /proc/stat 快照之间，一组 CPU 的平均忙碌百分比（非 idle/iowait）。
cpu_busy() {
	awk -v list="$3" '
		BEGIN { n = split(list, a, " "); for (i = 1; i <= n; i++) want["cpu" a[i]] = 1 }
		FNR == NR { if ($1 in want) { t0[$1] = $2+$3+$4+$5+$6+$7+$8+$9; i0[$1] = $5+$6 } next }
		($1 in want) { dt += $2+$3+$4+$5+$6+$7+$8+$9 - t0[$1]; di += $5+$6 - i0[$1] }
		END { printf "%.0f", dt ? 100 * (dt - di) / dt : 0 }' "$1" "$2"
}

rss_kb() { awk '/^VmRSS:/ { print $2 }' "/proc/$SERVER_PID/status"; }

CSV=$OUT/points.csv
[[ -f $CSV ]] || echo "server,size,pipeline,conns,json,server_cpu_pct,client_cpu_pct,rss_kb" >"$CSV"

npoints=$(($(wc -w <<<"$SIZES") * $(wc -w <<<"$PIPELINES") * $(wc -w <<<"$CONNS")))
echo "[$SERVER] $npoints points x ${TEST_TIME}s, server cpus $SERVER_CPUS, client cpus $CLIENT_CPUS"
started=$SECONDS
before=$(mktemp) after=$(mktemp)

for size in $SIZES; do
	start_server
	# 预填充：每个连接写一段连续的 key，覆盖全部 KEYS 个 key，之后的 GET 全部命中。
	memtier --ratio=1:0 --key-pattern=P:P --requests=allkeys --data-size="$size" --pipeline=64 \
		--threads="$CLIENT_THREADS" --clients=4 >"$OUT/$SERVER/prefill_s$size.txt" 2>&1
	rss=$(rss_kb)
	# 预热：让 Garnet 的 JIT 分层编译走完 GET 路径；两边一样跑，不计入结果。
	memtier --ratio="$RATIO" --key-pattern=R:R --data-size="$size" --pipeline=16 \
		--threads="$CLIENT_THREADS" --clients=8 --test-time="$WARMUP_TIME" >/dev/null 2>&1
	echo "[$SERVER] size=$size prefilled ${KEYS} keys, rss=$((rss / 1024))MB ($((SECONDS - started))s)"

	for pipeline in $PIPELINES; do
		for conns in $CONNS; do
			threads=$((conns < CLIENT_THREADS ? conns : CLIENT_THREADS))
			name=s${size}_p${pipeline}_c${conns}
			memtier --ratio="$RATIO" --key-pattern=R:R --distinct-client-seed --data-size="$size" \
				--pipeline="$pipeline" --threads="$threads" --clients=$((conns / threads)) \
				--test-time="$TEST_TIME" --print-percentiles=50,99,99.9 \
				--json-out-file="$OUT/$SERVER/$name.json" >"$OUT/$SERVER/$name.txt" 2>&1 &
			mt_pid=$!
			# CPU 只在稳态区间采样，避开建连和收尾
			sleep 1
			grep -E '^cpu[0-9]+ ' /proc/stat >"$before"
			sleep $((TEST_TIME - 2))
			grep -E '^cpu[0-9]+ ' /proc/stat >"$after"
			if ! wait "$mt_pid"; then
				echo "[$SERVER] $name: memtier failed, see $OUT/$SERVER/$name.txt" >&2
			fi
			if ! kill -0 "$SERVER_PID" 2>/dev/null; then
				echo "[$SERVER] server died during $name" >&2
				exit 1
			fi
			scpu=$(cpu_busy "$before" "$after" "$SERVER_CPU_LIST")
			ccpu=$(cpu_busy "$before" "$after" "$CLIENT_CPU_LIST")
			echo "$SERVER,$size,$pipeline,$conns,$SERVER/$name.json,$scpu,$ccpu,$rss" >>"$CSV"
			ops=$(awk '/^Totals/ { print $2 }' "$OUT/$SERVER/$name.txt")
			printf '[%s] %-16s %14s ops/s  server cpu %3s%%  client cpu %3s%%  (%ss)\n' \
				"$SERVER" "$name" "${ops:-?}" "$scpu" "$ccpu" "$((SECONDS - started))"
		done
	done
	stop_server
done
rm -f "$before" "$after"
echo "[$SERVER] done in $((SECONDS - started))s"
