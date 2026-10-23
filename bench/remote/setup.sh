#!/usr/bin/env bash
# 在压测机上准备 memtier_benchmark、.NET 运行时和 Garnet，全部装在 $BENCH_DIR 下（apt 依赖除外）。
# 可重复执行，已就绪的部分直接跳过。
set -euo pipefail

BENCH_DIR=${BENCH_DIR:-/root/fastest-bench}
MEMTIER_TAG=${MEMTIER_TAG:-2.5.2}
GARNET_VERSION=${GARNET_VERSION:-2.2.4}
DOTNET_CHANNEL=${DOTNET_CHANNEL:-10.0}

mkdir -p "$BENCH_DIR/tools" "$BENCH_DIR/src"
cd "$BENCH_DIR"

# ---------- memtier_benchmark：同一个压测客户端同时支持 memcache_text 和 redis 协议 ----------
if [[ ! -x tools/memtier_benchmark ]]; then
	deps=(build-essential autoconf automake libtool pkg-config libevent-dev libssl-dev zlib1g-dev git curl xz-utils)
	missing=()
	for p in "${deps[@]}"; do
		dpkg -s "$p" >/dev/null 2>&1 || missing+=("$p")
	done
	if ((${#missing[@]})); then
		DEBIAN_FRONTEND=noninteractive apt-get install -y "${missing[@]}"
	fi
	rm -rf src/memtier
	git clone --depth 1 --branch "$MEMTIER_TAG" https://github.com/RedisLabs/memtier_benchmark src/memtier
	(cd src/memtier && autoreconf -ivf && ./configure && make -j"$(nproc)") >src/memtier-build.log 2>&1 ||
		{ tail -30 src/memtier-build.log; exit 1; }
	cp src/memtier/memtier_benchmark tools/
fi
echo "memtier: $(tools/memtier_benchmark --version | head -1)"

# ---------- .NET 运行时（Garnet 发布包是框架依赖的单文件程序） ----------
if ! { [[ -x dotnet/dotnet ]] && dotnet/dotnet --list-runtimes | grep -q "Microsoft.NETCore.App $DOTNET_CHANNEL"; }; then
	curl -fsSL https://dot.net/v1/dotnet-install.sh -o tools/dotnet-install.sh
	bash tools/dotnet-install.sh --runtime dotnet --channel "$DOTNET_CHANNEL" --install-dir "$BENCH_DIR/dotnet" --no-path
fi
echo "dotnet: $(dotnet/dotnet --list-runtimes | grep Microsoft.NETCore.App | tail -1)"

# ---------- Garnet ----------
garnet_dir="garnet/$GARNET_VERSION"
if [[ ! -x "$garnet_dir/GarnetServer" ]]; then
	rm -rf "$garnet_dir" && mkdir -p "$garnet_dir"
	curl -fsSL -o "$garnet_dir.tar.xz" \
		"https://github.com/microsoft/garnet/releases/download/v$GARNET_VERSION/linux-x64-based.tar.xz"
	# 包里只有一层 net<版本>/ 目录
	tar -xJf "$garnet_dir.tar.xz" -C "$garnet_dir" --strip-components=1
	rm -f "$garnet_dir.tar.xz"
fi
echo "garnet: $GARNET_VERSION ($garnet_dir/GarnetServer)"
