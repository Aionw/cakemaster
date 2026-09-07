#!/usr/bin/env python3
"""User-space perf profiles; invoked by compare.sh ... profile, inside namespaces.

Stat and DWARF recording use separate runs. Enable/disable acknowledgements bound
both windows; server startup and the separate warmup client are excluded. Perf's
helper runs on a third CPU. Raw perf.data stays in the chosen output directory.
"""
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

from compare import command, cpu_seconds


class Perf:
    def __init__(self, kind, pid, prefix, cpu):
        self.kind = kind
        self.prefix = prefix
        read_control, self.control = os.pipe()
        self.ack, write_ack = os.pipe()
        args = ["taskset", "-c", str(cpu), "perf", kind, "-p", str(pid),
                "-D", "-1", "--control", f"fd:{read_control},{write_ack}"]
        if kind == "stat":
            args += ["-x", ";", "-o", str(prefix) + ".stat.csv", "-e",
                     "task-clock:u,cycles:u,instructions:u,branches:u,branch-misses:u,cache-misses:u"]
        else:
            args += ["-e", "cycles:u", "-F", "997", "--call-graph", "dwarf,16384",
                     "-o", str(prefix) + ".data"]
        self.log = open(str(prefix) + f".{kind}.log", "w")
        try:
            self.process = subprocess.Popen(args, pass_fds=(read_control, write_ack),
                                            stdout=self.log, stderr=subprocess.STDOUT)
        except BaseException:
            os.close(self.control)
            os.close(self.ack)
            self.log.close()
            raise
        finally:
            os.close(read_control)
            os.close(write_ack)

    def send(self, instruction):
        os.write(self.control, (instruction + "\n").encode())
        ready = select.select([self.ack], [], [], 10)[0]
        # perf versions can include the C string's trailing NUL in their ack.
        response = os.read(self.ack, 4096) if ready else b""
        if response.rstrip(b"\n\0") != b"ack":
            raise RuntimeError(f"perf {instruction} ack={response!r}; see {self.prefix}.{self.kind}.log")

    def stop(self):
        try:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                try:
                    self.process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
                    raise RuntimeError("perf did not stop")
            if self.process.returncode not in (0, -signal.SIGINT):
                raise RuntimeError(f"perf exited {self.process.returncode}; see {self.prefix}.{self.kind}.log")
        finally:
            os.close(self.control)
            os.close(self.ack)
            self.log.close()


def main():
    root, library, out = map(Path, sys.argv[1:])
    server_cpu = int(os.environ.get("SERVER_CPU", "2"))
    client_cpu = int(os.environ.get("CLIENT_CPU", "4"))
    perf_cpu = int(os.environ.get("PERF_CPU", "6"))
    if len({server_cpu, client_cpu, perf_cpu}) != 3:
        raise ValueError("server, client and perf need distinct CPUs")
    server_tunables = os.environ.get("SERVER_GLIBC_TUNABLES")
    backends = os.environ.get("PROFILE_BACKENDS", "kernel,dpdk-af-packet").split(",")
    if any(value not in ("kernel", "dpdk-af-packet") for value in backends):
        raise ValueError("unknown PROFILE_BACKENDS")
    ini = out / "af-packet.ini"
    ini.write_text((root / "interop/fstack/af-packet.ini").read_text().replace(
        "lcore_mask=4\n", f"lcore_mask={1 << server_cpu:x}\n"))
    (out / "environment.json").write_text(json.dumps({
        "scope": "USER SPACE ONLY; no kernel/off-CPU stacks; AF_PACKET software PMD",
        "perf": command("perf", "--version", capture_output=True).stdout.strip(),
        "perf_event_paranoid": Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip(),
        "kptr_restrict": Path("/proc/sys/kernel/kptr_restrict").read_text().strip(),
        "server_cpu": server_cpu, "client_cpu": client_cpu, "perf_cpu": perf_cpu,
        "server_glibc_tunables": server_tunables,
        "case_filter": os.environ.get("PROFILE_CASES"), "backends": backends,
        "record": "cycles:u, 997 Hz, DWARF stack 16384; stat is a separate pass",
        "sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in [
            library, root / "target/release/examples/benchmark", root / "target/release/mooncake_benchmark"]},
    }, indent=2) + "\n")
    cases = [("add", 1, 1000000), ("exists", 32, 3000000),
             ("exists", 256, 3000000), ("get", 32, 1000000), ("idle", 0, 0)]
    if selected := os.environ.get("PROFILE_CASES"):
        names = selected.split(",")
        if any(name not in [f"{w}-p{p}" for w, p, _ in cases] for name in names):
            raise ValueError("unknown PROFILE_CASES")
        cases = [case for case in cases if f"{case[0]}-p{case[1]}" in names]
    samples = []
    recordings = []
    for backend in backends:
        for workload, pipeline, default_count in cases:
            count = int(os.environ.get("PROFILE_ITERATIONS", str(default_count))) if workload != "idle" else 0
            if workload != "idle" and count <= 0:
                raise ValueError("PROFILE_ITERATIONS must be positive")
            name = f"{backend}-{workload}-p{pipeline}"
            binary = root / ("target/release/examples/benchmark" if workload in ("add", "idle")
                             else "target/release/mooncake_benchmark")
            address = "198.18.0.2:19092"
            if backend == "kernel":
                command("ip", "netns", "exec", "server", "ip", "addr", "add", "198.18.0.2/24", "dev", "dpdk0")
                server_args = [str(binary), "server", address, "1"]
            else:
                server_args = [str(binary), "server-dpdk", str(library), str(ini), address]
            if server_tunables is not None:
                server_args = ["env", f"GLIBC_TUNABLES={server_tunables}"] + server_args
            with (out / f"{name}.server.log").open("w") as log:
                server = subprocess.Popen(["ip", "netns", "exec", "server", "taskset", "-c", str(server_cpu)] + server_args,
                                          stdout=log, stderr=subprocess.STDOUT)
                try:
                    def client(iterations, depth):
                        args = ["taskset", "-c", str(client_cpu), str(binary), "client", address]
                        if workload not in ("add", "idle"):
                            args += [workload, "16"]
                        return args + [str(iterations), str(depth), "0"]

                    for _ in range(60):
                        if server.poll() is not None:
                            raise RuntimeError(f"server exited; see {name}.server.log")
                        try:
                            command(*client(1, 1), capture_output=True, timeout=1)
                            break
                        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                            time.sleep(0.1)
                    else:
                        raise RuntimeError("server not ready")
                    if workload != "idle":
                        command(*client(100000, pipeline), capture_output=True, timeout=20)
                    for kind in ("stat", "record"):
                        profiler = Perf(kind, server.pid, out / name, perf_cpu)
                        try:
                            profiler.send("enable")
                            cpu_before = cpu_seconds(server.pid)
                            start = time.monotonic()
                            if workload == "idle":
                                time.sleep(3)
                                raw = "idle, no client connections"
                            else:
                                result = subprocess.run(client(count, pipeline), capture_output=True, text=True, timeout=120)
                                (out / f"{name}.{kind}.client.log").write_text(result.stdout + result.stderr)
                                result.check_returncode()
                                raw = result.stdout.strip()
                            wall = time.monotonic() - start
                            cpu = cpu_seconds(server.pid) - cpu_before
                            profiler.send("disable")
                            sample = dict(backend=backend, workload=workload, pipeline=pipeline, kind=kind,
                                          iterations=count, wall_s=wall, server_cpu_s=cpu, client=raw)
                            samples.append(sample)
                            print(json.dumps(sample), flush=True)
                            (out / "runs.json").write_text(json.dumps(samples, indent=2) + "\n")
                        finally:
                            profiler.stop()
                        if kind == "record":
                            recordings.append(out / name)
                finally:
                    if server.poll() is None:
                        server.send_signal(signal.SIGINT)
                        try:
                            server.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            server.kill()
                            server.wait()
                            raise RuntimeError("server shutdown timed out")
                    if backend == "kernel":
                        command("ip", "netns", "exec", "server", "ip", "addr", "del", "198.18.0.2/24", "dev", "dpdk0")
                    if server.returncode != 0:
                        raise RuntimeError(f"server shutdown status {server.returncode}")
    # Symbolication is deliberately outside the profiling/measurement windows.
    for prefix in recordings:
        common = ["perf", "report", "--stdio", "-i", str(prefix) + ".data"]
        for suffix, options in [
            ("flat.txt", ["--no-children", "-g", "none", "--percent-limit", "0.2", "-n", "-s", "dso,symbol"]),
            ("callgraph.txt", ["--children", "-g", "graph,1,caller", "--percent-limit", "1"]),
            ("header.txt", ["--header-only"]),
        ]:
            with open(str(prefix) + "." + suffix, "w") as report:
                # An idle kernel server can have zero samples; retain that report.
                result = subprocess.run(common + options, stdout=report, stderr=subprocess.STDOUT, check=False)
                if not prefix.name.endswith("idle-p0"):
                    result.check_returncode()


if __name__ == "__main__":
    main()
