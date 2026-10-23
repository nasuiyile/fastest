#!/usr/bin/env python3
"""把 remote/run.sh 的结果目录汇总成 Markdown 报告（stdout）和 summary.csv。

用法: report.py <结果目录>
"""
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

SERVERS = ("fastest", "garnet")
LABELS = {"fastest": "fastest", "garnet": "Garnet"}


def read_kv(path):
    if not path.exists():
        return {}
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def load_point(root, row):
    path = root / row["json"]
    if not path.exists():
        return None
    stats = json.loads(path.read_text())["ALL STATS"]
    totals, gets = stats["Totals"], stats["Gets"]
    pct = totals.get("Percentile Latencies", {})
    lookups = gets["Hits/sec"] + gets["Misses/sec"]
    return {
        "ops": totals["Ops/sec"],
        "avg": totals["Average Latency"],
        "p50": pct.get("p50.00"),
        "p99": pct.get("p99.00"),
        "p999": pct.get("p99.90"),
        "kbps": totals["KB/sec"],
        "hit": gets["Hits/sec"] / lookups if lookups else None,
        "server_cpu": int(row["server_cpu_pct"]),
        "client_cpu": int(row["client_cpu_pct"]),
        "rss_kb": int(row["rss_kb"]),
    }


def fmt_ops(v):
    if v is None:
        return "-"
    if v >= 1e6:
        return f"{v / 1e6:.2f}M"
    return f"{v / 1e3:.1f}k"


def fmt_ms(v):
    return "-" if v is None else f"{v:.3f}"


def fmt_size(n):
    return f"{n // 1024}KB" if n >= 1024 else f"{n}B"


def geomean(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def main():
    root = Path(sys.argv[1])
    points = {}  # (server, size, pipeline, conns) -> metrics
    with open(root / "points.csv") as f:
        for row in csv.DictReader(f):
            key = (row["server"], int(row["size"]), int(row["pipeline"]), int(row["conns"]))
            points[key] = load_point(root, row)

    grid = sorted({k[1:] for k in points})
    sizes = sorted({g[0] for g in grid})
    pipelines = sorted({g[1] for g in grid})
    conns = sorted({g[2] for g in grid})

    ratios = {}
    for g in grid:
        f, gn = points.get(("fastest", *g)), points.get(("garnet", *g))
        if f and gn and gn["ops"]:
            ratios[g] = f["ops"] / gn["ops"]

    env = read_kv(root / "env.txt")
    params = read_kv(root / "fastest" / "params.txt") or read_kv(root / "garnet" / "params.txt")
    garnet = read_kv(root / "garnet" / "params.txt")

    out = []
    w = out.append
    w("# fastest vs Garnet 跑分\n")
    w("## 环境与参数\n")
    w("| 项 | 值 |")
    w("|---|---|")
    for k, label in (("date", "时间"), ("cpu", "CPU"), ("kernel", "内核"), ("mem", "内存"),
                     ("loadavg", "开跑前负载"), ("fastest", "fastest 版本"), ("memtier", "压测客户端"),
                     ("dotnet", ".NET 运行时")):
        if k in env:
            w(f"| {label} | {env[k]} |")
    w("| 协议 | fastest：memcached 文本协议（memtier `--protocol=memcache_text`）；"
      "Garnet：RESP（`--protocol=redis`，Garnet 不支持 memcached 协议） |")
    if garnet:
        w(f"| Garnet | {garnet.get('GARNET_VERSION')}，--memory {garnet.get('GARNET_MEMORY')}，"
          f"AOF/分层存储/checkpoint/compaction 全关 |")
    if params:
        w(f"| 服务端 CPU | {params['SERVER_CPUS']}（taskset） |")
        w(f"| 客户端 CPU | {params['CLIENT_CPUS']}，memtier {params['CLIENT_THREADS']} 线程 |")
        w(f"| 负载 | SET:GET = {params['RATIO']}，{int(params['KEYS']):,} 个 key 均匀随机访问，预填充后 GET 全部命中 |")
        w(f"| 每点时长 | {params['TEST_TIME']}s（每种 value 大小预填充后先预热 {params['WARMUP_TIME']}s，不计入） |")
    w("")

    w("## 汇总\n")
    w("比值 = fastest 吞吐 / Garnet 吞吐，大于 1 表示 fastest 更快；按维度取几何平均。\n")
    if ratios:
        wins = sum(r > 1 for r in ratios.values())
        w(f"- 全部 {len(ratios)} 个点的几何平均比值：**{geomean(ratios.values()):.2f}x**，fastest 更快的点：{wins}/{len(ratios)}")
    w("")
    w("| 维度 | 取值 | 比值 |")
    w("|---|---|---|")
    for name, idx, values, fmt in (("value 大小", 0, sizes, fmt_size), ("流水线深度", 1, pipelines, str),
                                   ("连接数", 2, conns, str)):
        for v in values:
            gm = geomean(r for g, r in ratios.items() if g[idx] == v)
            w(f"| {name} | {fmt(v)} | {'-' if gm is None else f'{gm:.2f}x'} |")
    w("")

    for size in sizes:
        w(f"## value = {fmt_size(size)}\n")
        w("| 流水线 | 连接 | fastest ops/s | Garnet ops/s | 比值 | fastest p50 / p99 / p99.9 ms | "
          "Garnet p50 / p99 / p99.9 ms | 服务端 CPU f / G | 客户端 CPU f / G |")
        w("|---:|---:|---:|---:|---:|---|---|---|---|")
        for p in pipelines:
            for c in conns:
                f, gn = points.get(("fastest", size, p, c)), points.get(("garnet", size, p, c))
                r = ratios.get((size, p, c))
                lat = lambda m: "-" if not m else f"{fmt_ms(m['p50'])} / {fmt_ms(m['p99'])} / {fmt_ms(m['p999'])}"
                cpu = lambda k: f"{f[k] if f else '-'}% / {gn[k] if gn else '-'}%"
                w(f"| {p} | {c} | {fmt_ops(f and f['ops'])} | {fmt_ops(gn and gn['ops'])} | "
                  f"{'-' if r is None else f'**{r:.2f}x**' if r > 1 else f'{r:.2f}x'} | {lat(f)} | {lat(gn)} | "
                  f"{cpu('server_cpu')} | {cpu('client_cpu')} |")
        w("")

    w("## 内存（预填充全部 key 后服务端 RSS）\n")
    w("| value 大小 | fastest | Garnet |")
    w("|---|---:|---:|")
    for size in sizes:
        cells = []
        for s in SERVERS:
            m = next((points[k] for k in sorted(points) if k[0] == s and k[1] == size and points[k]), None)
            cells.append(f"{m['rss_kb'] / 1024:,.0f} MB" if m else "-")
        w(f"| {fmt_size(size)} | {cells[0]} | {cells[1]} |")
    w("")

    misses = [(k, m["hit"]) for k, m in sorted(points.items()) if m and m["hit"] is not None and m["hit"] < 0.99]
    if misses:
        w("## 注意\n")
        for (s, size, p, c), hit in misses:
            w(f"- {LABELS[s]} {fmt_size(size)} p{p} c{c}: GET 命中率只有 {hit:.1%}，预填充可能不完整")
        w("")

    print("\n".join(out))

    with open(root / "summary.csv", "w", newline="") as f:
        cw = csv.writer(f)
        cw.writerow(["size", "pipeline", "conns", "server", "ops", "p50_ms", "p99_ms", "p999_ms", "avg_ms",
                     "kb_per_sec", "get_hit_ratio", "server_cpu_pct", "client_cpu_pct", "rss_kb"])
        for (s, size, p, c), m in sorted(points.items(), key=lambda kv: (kv[0][1:], kv[0][0])):
            if m:
                cw.writerow([size, p, c, s, f"{m['ops']:.0f}", m["p50"], m["p99"], m["p999"], m["avg"],
                             m["kbps"], m["hit"], m["server_cpu"], m["client_cpu"], m["rss_kb"]])


if __name__ == "__main__":
    main()
