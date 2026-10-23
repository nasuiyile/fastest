#!/usr/bin/env python3
"""汇总 remote/ab.sh 的结果（Markdown 输出到 stdout）。

用法: ab_report.py <结果目录>/sweep.csv

每个 (标签, value 大小, 测试点) 取各轮吞吐的中位数，列出 new / base 和 new/base（> 1 表示改动后更快），
以及每一轮的比值，用来判断差距是否超出轮间波动。
bench/results/share-* 等目录是小 value 拷贝实验（已回退）的旧结果，里面的 copy / share 模式同样能汇总。
"""
import csv
import statistics
import sys
from collections import defaultdict


def size_label(size):
    return f"{size // 1024}KB" if size >= 1024 and size % 1024 == 0 else f"{size}B"


def main(path):
    ops = defaultdict(list)
    cpu = defaultdict(list)
    keys = {}
    for row in csv.DictReader(open(path)):
        key = (row["tag"], int(row["size"]), int(row["pipeline"]), int(row["conns"]), row["mode"])
        ops[key].append(int(row["ops"]))
        cpu[key].append(int(row["server_cpu_pct"]))
        keys[(row["tag"], int(row["size"]))] = int(row["keys"])

    groups = sorted({k[:4] for k in ops})
    for tag in sorted({g[0] for g in groups}):
        for pipeline, conns in sorted({(g[2], g[3]) for g in groups if g[0] == tag}):
            print(f"\n### {tag}，流水线 {pipeline}，{conns} 连接\n")
            modes = [m for m in ("copy", "share", "new", "base") if any(k[0] == tag and k[2] == pipeline and k[3] == conns
                                                                        and k[4] == m for k in ops)]
            ratios = [(a, b) for a, b in (("copy", "share"), ("share", "base"), ("new", "base")) if a in modes and b in modes]
            print("| value | key 数 | " + " | ".join(f"{m} ops/s" for m in modes) + " | " +
                  " | ".join(f"{a}/{b}" for a, b in ratios) + " | 各轮 " + "，".join(f"{a}/{b}" for a, b in ratios) + " |")
            print("|---|---|" + "---|" * (len(modes) + len(ratios) + 1))
            for size in sorted({g[1] for g in groups if g[0] == tag and g[2] == pipeline and g[3] == conns}):
                med = {}
                for mode in modes:
                    values = ops.get((tag, size, pipeline, conns, mode))
                    med[mode] = statistics.median(values) if values else None

                def per_round(a, b):
                    runs_a = ops.get((tag, size, pipeline, conns, a), [])
                    runs_b = ops.get((tag, size, pipeline, conns, b), [])
                    return " ".join(f"{x / y:.3f}" for x, y in zip(runs_a, runs_b))

                def fmt(value):
                    return f"{value:,.0f}" if value else "-"

                def ratio(a, b):
                    return f"{med[a] / med[b]:.3f}" if med[a] and med[b] else "-"

                print(f"| {size_label(size)} | {keys[(tag, size)]:,} | " + " | ".join(fmt(med[m]) for m in modes) + " | " +
                      " | ".join(ratio(a, b) for a, b in ratios) + " | " +
                      "；".join(per_round(a, b) for a, b in ratios) + " |")


if __name__ == "__main__":
    main(sys.argv[1])
