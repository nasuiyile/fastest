#!/usr/bin/env bash
# fastest vs Garnet 跑分入口，在开发机上执行：
#   编译 fastest（Release）→ 上传到压测机 → 准备 memtier/.NET/Garnet → 依次压测两个服务端 → 取回结果 → 生成报告。
#
#   BENCH_HOST=192.168.31.29 BENCH_PASS=<root 密码> bench/bench.sh
#
# 没有 BENCH_PASS 时走 ssh 自己的认证（密钥、agent）。
# 压测参数（SIZES、PIPELINES、CONNS、TEST_TIME、SERVER_CPUS 等）原样传给 remote/run.sh，默认值见那里。
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BENCH_HOST=${BENCH_HOST:?set BENCH_HOST}
BENCH_USER=${BENCH_USER:-root}
BENCH_PORT=${BENCH_PORT:-22}
REMOTE_DIR=${REMOTE_DIR:-/root/fastest-bench}
BUILD_DIR=${BUILD_DIR:-$ROOT/cmake-build-bench}
VCPKG_ROOT=${VCPKG_ROOT:-$HOME/vcpkg}
SERVERS=${SERVERS:-"fastest garnet"}
SKIP_BUILD=${SKIP_BUILD:-0}

ssh_common=(-o StrictHostKeyChecking=accept-new -o ControlMaster=auto -o "ControlPath=/tmp/fastest-bench-%C"
	-o ControlPersist=10m)
target=$BENCH_USER@$BENCH_HOST

if [[ -n ${BENCH_PASS:-} ]]; then
	# 密码只经环境变量交给 askpass，不落盘。
	askpass=$(mktemp)
	printf '#!/bin/sh\nprintf "%%s\\n" "$BENCH_PASS"\n' >"$askpass"
	chmod 700 "$askpass"
	export BENCH_PASS SSH_ASKPASS=$askpass SSH_ASKPASS_REQUIRE=force
fi
cleanup() {
	ssh "${ssh_common[@]}" -p "$BENCH_PORT" -O exit "$target" 2>/dev/null || true
	[[ -z ${askpass:-} ]] || rm -f "$askpass"
}
trap cleanup EXIT

remote() { ssh "${ssh_common[@]}" -p "$BENCH_PORT" "$target" "$@" </dev/null; }
upload() { scp "${ssh_common[@]}" -P "$BENCH_PORT" "$@" </dev/null; }

# 本地设置过的压测参数原样带到远端
remote_env="BENCH_DIR=$REMOTE_DIR"
for v in SIZES PIPELINES CONNS TEST_TIME WARMUP_TIME KEYS RATIO SERVER_CPUS CLIENT_CPUS CLIENT_THREADS \
	GARNET_VERSION GARNET_MEMORY MEMTIER_TAG DOTNET_CHANNEL; do
	[[ -z ${!v:-} ]] || remote_env+=" $v=$(printf %q "${!v}")"
done

if ((!SKIP_BUILD)); then
	cmake -S "$ROOT" -B "$BUILD_DIR" -Wno-dev -DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" >/dev/null
	cmake --build "$BUILD_DIR" --target fastest -j
fi

stamp=$(date +%Y%m%d-%H%M%S)
out=$REMOTE_DIR/results/$stamp
remote "mkdir -p $out"
upload "$BUILD_DIR/fastest" "$ROOT/bench/remote/setup.sh" "$ROOT/bench/remote/run.sh" "$target:$REMOTE_DIR/"
remote "$remote_env bash $REMOTE_DIR/setup.sh"

rev=$(git -C "$ROOT" rev-parse --short HEAD)
git -C "$ROOT" diff --quiet --ignore-cr-at-eol HEAD -- src CMakeLists.txt || rev+="-dirty"
remote "export LC_ALL=C; cd $REMOTE_DIR && {
	echo \"date=\$(date '+%F %T %z')\"
	echo \"cpu=\$(lscpu | sed -n 's/^Model name: *//p')\"
	echo \"kernel=\$(uname -r)\"
	echo \"mem=\$(free -g | awk 'NR == 2 { print \$2 }')GB\"
	echo \"memtier=\$(tools/memtier_benchmark --version | head -1)\"
	echo \"dotnet=\$(dotnet/dotnet --list-runtimes | grep Microsoft.NETCore.App | tail -1 | cut -d' ' -f2)\"
	echo \"fastest=$rev\"
	echo \"loadavg=\$(cut -d' ' -f1-3 /proc/loadavg)\"
} > $out/env.txt"

for server in $SERVERS; do
	remote "$remote_env bash $REMOTE_DIR/run.sh $server $out"
done

mkdir -p "$ROOT/bench/results"
scp "${ssh_common[@]}" -P "$BENCH_PORT" -r "$target:$out" "$ROOT/bench/results/" </dev/null
python3 "$ROOT/bench/report.py" "$ROOT/bench/results/$stamp" >"$ROOT/bench/results/$stamp/report.md"
echo "report: bench/results/$stamp/report.md"
