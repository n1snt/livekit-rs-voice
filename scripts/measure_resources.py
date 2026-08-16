#!/usr/bin/env python3
"""Sample memory + CPU of the Rust and Go LiveKit servers side by side.

Rust server: a host process, measured via `ps` (RSS + cumulative CPU time).
Go server: a Docker container with cgroup v2, measured via
  /sys/fs/cgroup/memory.current, memory.stat (anon), cpu.stat (usage_usec).

Usage:
  measure_resources.py --rust-pid <pid> --go-container <name> [--interval 2] [--duration 20]

Outputs a per-interval table and a summary of deltas.
"""

import argparse
import subprocess
import time


def rust_sample(pid):
    out = subprocess.check_output(["ps", "-o", "rss=,time=", "-p", str(pid)]).decode().strip()
    parts = out.split()
    rss_kb = int(parts[0])
    cpu_time = parts[1]  # [MM:]SS.CC
    seg = cpu_time.split(":")
    if len(seg) == 3:
        cpu_s = float(seg[0]) * 3600 + float(seg[1]) * 60 + float(seg[2])
    elif len(seg) == 2:
        cpu_s = float(seg[0]) * 60 + float(seg[1])
    else:
        cpu_s = float(seg[0])
    return rss_kb, cpu_s


def go_sample(container):
    base = ["docker", "exec", container, "sh", "-c"]
    mem = int(subprocess.check_output(base + ["cat /sys/fs/cgroup/memory.current"]).decode().strip())
    stat = subprocess.check_output(base + ["cat /sys/fs/cgroup/memory.stat"]).decode()
    anon = 0
    for line in stat.splitlines():
        if line.startswith("anon "):
            anon = int(line.split()[1])
    cpu_us = int(subprocess.check_output(base + ["cat /sys/fs/cgroup/cpu.stat"]).decode().splitlines()[0].split()[1])
    return mem, anon, cpu_us


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rust-pid", type=int, required=True)
    ap.add_argument("--go-container", required=True)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    # Initial samples for delta computation.
    r0 = rust_sample(args.rust_pid)
    g0 = go_sample(args.go_container)
    t0 = time.time()

    print(f"== resource sampling: {args.label} ==")
    print(f"{'t(s)':>6} | {'rust RSS(MB)':>13} {'rust CPU%':>9} | "
          f"{'go mem.current(MB)':>19} {'go anon(MB)':>12} {'go CPU%':>8}")
    print("-" * 74)
    start = time.time()
    last_r, last_g = r0, g0
    last_wall = start
    n = 0
    while time.time() - start < args.duration:
        time.sleep(args.interval)
        t = time.time() - start
        r = rust_sample(args.rust_pid)
        g = go_sample(args.go_container)
        dt = t - last_wall
        rust_cpu = ((r[1] - last_r[1]) / dt) * 100 if dt > 0 else 0
        go_cpu = ((g[2] - last_g[2]) / 1e6 / dt) * 100 if dt > 0 else 0
        print(f"{t:6.1f} | {r[0] / 1024:13.1f} {rust_cpu:9.1f} | "
              f"{g[0] / 1024 / 1024:19.1f} {g[1] / 1024 / 1024:12.1f} {go_cpu:8.1f}")
        last_r, last_g, last_wall = r, g, t
        n += 1

    # Summary (deltas over the whole window).
    r = rust_sample(args.rust_pid)
    g = go_sample(args.go_container)
    dt = time.time() - start
    rust_delta = r[1] - r0[1]
    go_delta = (g[2] - g0[2]) / 1e6
    print("-" * 74)
    print(f"summary | rust RSS {r[0] / 1024:.0f} MB (+{(r[0] - r0[0]) / 1024:.0f}), "
          f"CPU window {rust_delta:.2f}s over {dt:.1f}s ({rust_delta / dt * 100:.1f}% avg) | "
          f"go anon {g[1] / 1024 / 1024:.0f} MB (+{(g[1] - g0[1]) / 1024 / 1024:.0f}), "
          f"CPU window {go_delta:.2f}s ({go_delta / dt * 100:.1f}% avg)")


if __name__ == "__main__":
    main()
