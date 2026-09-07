#!/usr/bin/env python3
"""Invoked inside compare.sh's disposable namespaces, not directly on the host."""
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import statistics
import subprocess
import sys
import time


def command(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def main():
    root, library, out = map(Path, sys.argv[1:])
    server_cpu = int(os.environ.get("SERVER_CPU", "2"))
    client_cpu = int(os.environ.get("CLIENT_CPU", "4"))
    rounds = int(os.environ.get("ROUNDS", "3"))
    iterations = int(os.environ.get("ITERATIONS", "100000"))
    warmup = int(os.environ.get("WARMUP", "10000"))
    pipelines = [int(value) for value in os.environ.get("PIPELINES", "1,32,256").split(",")]
    workloads = os.environ.get("WORKLOADS", "add,get,exists").split(",")
    if not workloads or any(value not in ("add", "get", "exists") for value in workloads):
        raise ValueError("WORKLOADS must be a subset of add,get,exists")
    server_tunables = os.environ.get("SERVER_GLIBC_TUNABLES")
    if min(rounds, iterations, warmup, *pipelines) <= 0 or server_cpu == client_cpu:
        raise ValueError("positive counts and distinct CPUs required")
    config = (root / "interop/fstack/af-packet.ini").read_text()
    config = config.replace("lcore_mask=4\n", f"lcore_mask={1 << server_cpu:x}\n")
    ini = out / "af-packet.ini"
    ini.write_text(config)
    metadata = {
        "path": "veth / AF_PACKET software PMD, NOT physical NIC kernel bypass",
        "kernel": platform.platform(),
        "server_cpu": server_cpu, "client_cpu": client_cpu,
        "rounds": rounds, "iterations": iterations, "warmup": warmup,
        "pipelines": pipelines, "library": str(library),
        "workloads": workloads, "server_glibc_tunables": server_tunables,
        "rustc": command("rustc", "--version", capture_output=True).stdout.strip(),
        "lscpu": command("lscpu", "-e=CPU,CORE,SOCKET,ONLINE", capture_output=True).stdout,
        "cpu_info": command("lscpu", capture_output=True).stdout,
        "fstack_revision": "34065f1396c7695408066c4bccc9dc98c02f60dc + eal-argv.patch",
        "dpdk_version": "24.11.6 (bundled)",
        "sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in [
            library, root / "target/release/examples/benchmark",
            root / "target/release/mooncake_benchmark", root / "target/release/examples/fstack_probe",
        ]},
    }
    (out / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # Correctness checks run outside the measurement window with the real native
    # stack, including timers and data larger than TCP send/receive buffers.
    probe = root / "target/release/examples/fstack_probe"
    with (out / "probe-server.log").open("w") as log:
        process = subprocess.Popen(["ip", "netns", "exec", "server", str(probe), "server", str(library), str(ini), "198.18.0.2:19092"], stdout=log, stderr=subprocess.STDOUT)
        try:
            import socket
            for attempt in range(60):
                if process.poll() is not None:
                    raise RuntimeError("native probe server exited")
                try:
                    with socket.create_connection(("198.18.0.2", 19092), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("native probe server not ready")
            result = subprocess.run([str(probe), "client", "198.18.0.2:19092"], capture_output=True, text=True, timeout=30)
            (out / "probe-client.log").write_text(result.stdout + result.stderr)
            result.check_returncode()
            print(result.stdout, flush=True)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise RuntimeError("native probe shutdown timed out")
            if process.returncode != 0:
                raise RuntimeError(f"native probe shutdown status {process.returncode}")
    if os.environ.get("PROBE_ONLY") == "1":
        return
    samples = []
    for round_index in range(rounds):
        backends = ["kernel", "dpdk-af-packet"]
        if round_index % 2:
            backends.reverse()
        for backend in backends:
            for workload in workloads:
                binary = root / ("target/release/examples/benchmark" if workload == "add"
                                 else "target/release/mooncake_benchmark")
                prefix = ["ip", "netns", "exec", "server", "taskset", "-c", str(server_cpu)]
                addr = "198.18.0.2:19092"
                if backend == "kernel":
                    command("ip", "netns", "exec", "server", "ip", "addr", "add", "198.18.0.2/24", "dev", "dpdk0")
                    server_args = [str(binary), "server", addr, "1"]
                else:
                    server_args = [str(binary), "server-dpdk", str(library), str(ini), addr]
                # Apply only to the server, never to the fixed client or helpers.
                if server_tunables is not None:
                    server_args = ["env", f"GLIBC_TUNABLES={server_tunables}"] + server_args
                log_path = out / f"server-{round_index}-{backend}-{workload}.log"
                with log_path.open("w") as log:
                    process = subprocess.Popen(prefix + server_args, stdout=log, stderr=subprocess.STDOUT)
                    try:
                        def client(count, pipeline, warm):
                            args = ["taskset", "-c", str(client_cpu), str(binary), "client", addr]
                            if workload != "add":
                                args += [workload, "16"]
                            return args + [str(count), str(pipeline), str(warm)]

                        # Actual successful RPC, not just a log line or listening fd.
                        for attempt in range(60):
                            if process.poll() is not None:
                                raise RuntimeError(f"server exited; see {log_path}")
                            try:
                                command(*client(1, 1, 0), capture_output=True, timeout=1)
                                break
                            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                                time.sleep(0.1)
                        else:
                            raise RuntimeError(f"server not ready; see {log_path}")
                        for pipeline in pipelines:
                            before = cpu_seconds(process.pid)
                            started = time.monotonic()
                            output = command(*client(iterations, pipeline, warmup), capture_output=True, timeout=120).stdout.strip()
                            wall = time.monotonic() - started
                            cpu = cpu_seconds(process.pid) - before
                            values = dict(part.split("=", 1) for part in output.split() if "=" in part)
                            # mooncake_benchmark labels batch throughput as batch_qps.
                            qps = float(values.get("qps", values.get("batch_qps", "nan")))
                            sample = dict(round=round_index, backend=backend, workload=workload,
                                          pipeline=pipeline, qps=qps, raw=output,
                                          server_cpu_pct_including_warmup=100 * cpu / wall)
                            samples.append(sample)
                            print(json.dumps(sample), flush=True)
                            (out / "samples.json").write_text(json.dumps(samples, indent=2, allow_nan=False) + "\n")
                    finally:
                        if process.poll() is None:
                            process.send_signal(signal.SIGINT)
                            try:
                                process.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait()
                                raise RuntimeError(f"server shutdown timed out; see {log_path}")
                        if backend == "kernel":
                            command("ip", "netns", "exec", "server", "ip", "addr", "del", "198.18.0.2/24", "dev", "dpdk0")
                        if process.returncode != 0:
                            raise RuntimeError(f"server shutdown status {process.returncode}; see {log_path}")
    summary = []
    for workload in workloads:
        for pipeline in pipelines:
            row = dict(workload=workload, pipeline=pipeline)
            for backend in ["kernel", "dpdk-af-packet"]:
                matching = [s for s in samples if (s["workload"], s["pipeline"], s["backend"]) == (workload, pipeline, backend)]
                row[backend] = statistics.median(s["qps"] for s in matching)
            row["change_pct"] = 100 * (row["dpdk-af-packet"] / row["kernel"] - 1)
            summary.append(row)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
