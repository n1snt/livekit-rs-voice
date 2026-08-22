#!/usr/bin/env python3
"""Sample a running Docker container's memory + CPU via the Docker API.

Reads the container's cgroup stats from the Docker socket (`/containers/<id>/stats`),
so it works for any container, including shell-less distroless ones. Reports
`memory.usage` (cgroup `memory.current`), `anon` (anonymous RSS, the working-set
proxy), and CPU as a percentage of one core (delta of the cumulative
`cpu.usage.total_usage`).

Usage:
  sample_container.py --container <name> [--interval 0.5] [--duration 30] [--label x] [--stop-file path]

`--stop-file`: exit early (with a summary) once the file exists, letting the
harness stop sampling exactly when a recording completes.

Summary line: `summary label=<label> mem_peak=MB mem_anon_peak=MB cpu_avg=% cpu_max=%`
"""

import argparse
import json
import os
import subprocess
import time


def docker_socket():
    cands = [os.environ.get("DOCKER_HOST", "")]
    cands += ["$HOME/.docker/run/docker.sock", "/var/run/docker.sock", "/run/docker.sock"]
    for c in cands:
        p = os.path.expandvars(c).replace("unix://", "")
        if p and os.path.exists(p):
            return p
    raise SystemExit("cannot locate the Docker socket")


def stats(container):
    out = subprocess.check_output(
        [
            "curl", "-s", "--unix-socket", docker_socket(),
            f"http://localhost/containers/{container}/stats?stream=false",
        ]
    )
    d = json.loads(out)
    ms = d["memory_stats"]
    mem = ms.get("usage", 0)
    anon = ms.get("stats", {}).get("anon", mem)
    cpu = d["cpu_stats"]["cpu_usage"].get("total_usage", 0)
    return mem, anon, cpu


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", required=True)
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--label", default="")
    ap.add_argument("--stop-file", default="")
    args = ap.parse_args()

    m0, a0, c0 = stats(args.container)
    t0 = time.time()

    print(f"== resource sampling: {args.label} ({args.container}) ==")
    print(f"{'t(s)':>6} | {'mem.current(MB)':>16} {'anon(MB)':>10} {'CPU%':>7}")
    print("-" * 48)

    last_c = c0
    last_wall = t0
    mem_peak = m0
    anon_peak = a0
    cpu_avg_sum = 0.0
    cpu_max = 0.0
    n = 0
    while time.time() - t0 < args.duration:
        if args.stop_file and os.path.exists(args.stop_file):
            break
        time.sleep(args.interval)
        t = time.time() - t0
        m, a, c = stats(args.container)
        dt = t - last_wall
        cpu = ((c - last_c) / 1e9 / dt) * 100 if dt > 0 else 0.0
        mem_peak = max(mem_peak, m)
        anon_peak = max(anon_peak, a)
        cpu_max = max(cpu_max, cpu)
        cpu_avg_sum += cpu
        n += 1
        print(f"{t:6.1f} | {m / 1024 / 1024:16.1f} {a / 1024 / 1024:10.1f} {cpu:7.1f}")
        last_c, last_wall = c, t

    cpu_avg = cpu_avg_sum / n if n else 0.0
    print("-" * 48)
    print(
        f"summary label={args.label} "
        f"mem_peak={mem_peak / 1024 / 1024:.1f} "
        f"mem_anon_peak={anon_peak / 1024 / 1024:.1f} "
        f"cpu_avg={cpu_avg:.1f} "
        f"cpu_max={cpu_max:.1f}"
    )


if __name__ == "__main__":
    main()