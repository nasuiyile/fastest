#!/usr/bin/env bash
# 在压测机上对比改动前后的两个 fastest 二进制（A/B）。
# 用法: ab.sh <输出目录>
#
# 对每个 value 大小（一项），new（NEW_BIN NEW_ARGS）和 base（BASE_BIN BASE_ARGS）各自启动、预填充，跑同一组测试点；
# 每轮轮换两者的先后顺序，取各轮的中位数，抵消机器状态随时间的漂移。
# 默认参数让一项在 1 分钟内跑完：2 种服务端 × 2 轮 × (启动、预填充、预热约 3 秒 + 2 个点 × 4 秒) ≈ 45 秒。
# 结果：<输出目录>/sweep.csv（每个点一行）和 memtier 的原始输出，用 bench/ab_report.py 汇总。
set -euo pipefail

OUT=${1:?usage: ab.sh <out_dir>}
BENCH_DIR=${BENCH_DIR:-/root/fastest-bench}
NEW_BIN=${NEW_BIN:-$BENCH_DIR/fastest-new}
BASE_BIN=${BASE_BIN:-$BENCH_DIR/fastest-base}
read -ra NEW_ARGS <<<"${NEW_ARGS:-}"
read -ra BASE_ARGS <<<"${BASE_ARGS:-}"
SERVER_CPUS=${SERVER_CPUS:-0-7}        # 9950X：CCD0 的 8 个物理核
CLIENT_CPUS=${CLIENT_CPUS:-8-15,24-31} # CCD1 的 8 核 16 线程
CLIENT_THREADS=${CLIENT_THREADS:-16}
SIZES=${SIZES:-"64 256 1024 4096 16384"}
MODES=${MODES:-"new base"}
ROUNDS=${ROUNDS:-2}
POINTS=${POINTS:-"1:128 16:128"}     # 测试点，<流水线深度>:<连接数>
TEST_TIME=${TEST_TIME:-4}            # 每个点的秒数，至少 3（CPU 只在中间 TEST_TIME-2 秒采样）
WARMUP_TIME=${WARMUP_TIME:-1}        # fastest 没有 JIT，预热只是让连接和缓存进入稳态
MAX_KEYS=${MAX_KEYS:-1000000}
WORKING_SET=${WORKING_SET:-536870912}  # 大 value 时减少 key 数，数据总量不超过这么多字节（远大于 L3，预填充又快）
KEYS=${KEYS:-}                         # 固定 key 数（例如热点测试 KEYS=8），不设时按 MAX_KEYS / WORKING_SET 计算
TAG=${TAG:-uniform}                    # 写进 csv，区分不同的 key 分布和读写比例
RATIO=${RATIO:-0:1}                    # SET:GET，默认纯 GET
PORT=2333
MEMTIER=$BENCH_DIR/tools/memtier_benchmark

mkdir -p "$OUT/raw"
OUT=$(cd "$OUT" && pwd)
ulimit -n 65536

port_listening() { ss -Hltn "sport = :$PORT" | grep -q .; }

SERVER_PID=
stop_server() {
	if [[ -n $SERVER_PID ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
		kill "$SERVER_PID"
		wait "$SERVER_PID" 2>/dev/null || true
	fi
	SERVER_PID=
	for _ in $(seq 50); do
		port_listening || return 0
		sleep 0.1
	done
}
trap stop_server EXIT

start_server() {
	local mode=$1
	if port_listening; then
		echo "port $PORT is already in use" >&2
		exit 1
	fi
	local cmd
	case $mode in
	new) cmd=("$NEW_BIN" "${NEW_ARGS[@]}") ;;
	base) cmd=("$BASE_BIN" "${BASE_ARGS[@]}") ;;
	*) echo "unknown mode: $mode" >&2; exit 2 ;;
	esac
	taskset -c "$SERVER_CPUS" "${cmd[@]}" >>"$OUT/server.log" 2>&1 &
	SERVER_PID=$!
	for _ in $(seq 100); do
		(exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
		kill -0 "$SERVER_PID" 2>/dev/null || break
		sleep 0.1
	done
	echo "server ($mode) failed to start, see $OUT/server.log" >&2
	exit 1
}

memtier() {
	taskset -c "$CLIENT_CPUS" "$MEMTIER" -s 127.0.0.1 -p "$PORT" --protocol=memcache_text \
		--key-minimum=1 --key-maximum="$keys" --hide-histogram "$@"
}

expand_cpus() {
	local IFS=, part out=()
	for part in $1; do
		if [[ $part == *-* ]]; then out+=($(seq "${part%-*}" "${part#*-}")); else out+=("$part"); fi
	done
	echo "${out[@]}"
}
SERVER_CPU_LIST=$(expand_cpus "$SERVER_CPUS")
CLIENT_CPU_LIST=$(expand_cpus "$CLIENT_CPUS")

cpu_busy() {
	awk -v list="$3" '
		BEGIN { n = split(list, a, " "); for (i = 1; i <= n; i++) want["cpu" a[i]] = 1 }
		FNR == NR { if ($1 in want) { t0[$1] = $2+$3+$4+$5+$6+$7+$8+$9; i0[$1] = $5+$6 } next }
		($1 in want) { dt += $2+$3+$4+$5+$6+$7+$8+$9 - t0[$1]; di += $5+$6 - i0[$1] }
		END { printf "%.0f", dt ? 100 * (dt - di) / dt : 0 }' "$1" "$2"
}

CSV=$OUT/sweep.csv
[[ -f $CSV ]] || echo "tag,size,keys,mode,round,pipeline,conns,ops,p50,p99,server_cpu_pct,client_cpu_pct" >"$CSV"
{
	for v in SERVER_CPUS CLIENT_CPUS CLIENT_THREADS SIZES MODES ROUNDS POINTS TEST_TIME WARMUP_TIME \
		MAX_KEYS WORKING_SET KEYS TAG RATIO NEW_ARGS BASE_ARGS; do
		echo "$v=${!v}"
	done
	echo "new=$(sha256sum "$NEW_BIN" | cut -c1-12) base=$(sha256sum "$BASE_BIN" | cut -c1-12)"
} >>"$OUT/params.txt"

read -ra mode_list <<<"$MODES"
before=$(mktemp) after=$(mktemp)
started=$SECONDS
for size in $SIZES; do
	keys=${KEYS:-$((WORKING_SET / size < MAX_KEYS ? WORKING_SET / size : MAX_KEYS))}
	item_started=$SECONDS
	for round in $(seq "$ROUNDS"); do
		for i in "${!mode_list[@]}"; do
			mode=${mode_list[$(((i + round) % ${#mode_list[@]}))]}
			start_server "$mode"
			memtier --ratio=1:0 --key-pattern=P:P --requests=allkeys --data-size="$size" --pipeline=64 \
				--threads="$CLIENT_THREADS" --clients=4 >/dev/null 2>&1
			memtier --ratio="$RATIO" --key-pattern=R:R --data-size="$size" --pipeline=16 \
				--threads="$CLIENT_THREADS" --clients=8 --test-time="$WARMUP_TIME" >/dev/null 2>&1
			for point in $POINTS; do
				pipeline=${point%:*} conns=${point#*:}
				threads=$((conns < CLIENT_THREADS ? conns : CLIENT_THREADS))
				name=${TAG}_s${size}_${mode}_r${round}_p${pipeline}_c${conns}
				memtier --ratio="$RATIO" --key-pattern=R:R --distinct-client-seed --data-size="$size" \
					--pipeline="$pipeline" --threads="$threads" --clients=$((conns / threads)) \
					--test-time="$TEST_TIME" --print-percentiles=50,99 \
					--json-out-file="$OUT/raw/$name.json" >"$OUT/raw/$name.txt" 2>&1 &
				mt_pid=$!
				sleep 1
				grep -E '^cpu[0-9]+ ' /proc/stat >"$before"
				sleep $((TEST_TIME - 2))
				grep -E '^cpu[0-9]+ ' /proc/stat >"$after"
				wait "$mt_pid" || echo "$name: memtier failed" >&2
				kill -0 "$SERVER_PID" 2>/dev/null || { echo "server died during $name" >&2; exit 1; }
				read -r ops p50 p99 < <(python3 - "$OUT/raw/$name.json" <<'EOF'
import json, sys
t = json.load(open(sys.argv[1]))["ALL STATS"]["Totals"]
p = t.get("Percentile Latencies", {})
print(round(t["Ops/sec"]), p.get("p50.00", 0), p.get("p99.00", 0))
EOF
				)
				echo "$TAG,$size,$keys,$mode,$round,$pipeline,$conns,$ops,$p50,$p99,$(cpu_busy "$before" "$after" "$SERVER_CPU_LIST"),$(cpu_busy "$before" "$after" "$CLIENT_CPU_LIST")" >>"$CSV"
				printf '[%s] size=%-6s %-5s round %s p=%-3s c=%-4s %12s ops/s  (%ss)\n' \
					"$TAG" "$size" "$mode" "$round" "$pipeline" "$conns" "$ops" "$((SECONDS - started))"
			done
			stop_server
		done
	done
	echo "[$TAG] size=$size 一项用时 $((SECONDS - item_started))s"
done
rm -f "$before" "$after"
echo "[$TAG] done in $((SECONDS - started))s"
