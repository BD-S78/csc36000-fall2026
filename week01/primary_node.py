#!/usr/bin/env python3
"""
primary_node.py

Primary coordinator that:
1) Maintains an in-memory registry of secondary nodes (registered by secondary_node.py)
2) Distributes prime-range computation requests to registered secondary nodes
3) Aggregates results in memory and returns a final result (count or list sample)

Endpoints
---------
GET  /health
GET  /nodes
POST /register
POST /compute
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


class Registry:
    def __init__(self, ttl_s: int = 3600):
        self.ttl_s = ttl_s
        self.lock = threading.Lock()
        self.nodes: Dict[str, Dict[str, Any]] = {}

    def upsert(self, node: Dict[str, Any]) -> Dict[str, Any]:
        node_id = str(node["node_id"])
        now = time.time()
        record = {
            "node_id": node_id,
            "host": str(node["host"]),
            "port": int(node["port"]),
            "cpu_count": int(node.get("cpu_count", 1)),
            "last_seen": float(node.get("ts", now)),
            "registered_at": now,
        }
        with self.lock:
            if node_id in self.nodes:
                record["registered_at"] = self.nodes[node_id].get("registered_at", now)
            self.nodes[node_id] = record
            return record

    def active_nodes(self) -> List[Dict[str, Any]]:
        now = time.time()
        with self.lock:
            stale = [nid for nid, rec in self.nodes.items() if (now - float(rec.get("last_seen", 0))) > self.ttl_s]
            for nid in stale:
                del self.nodes[nid]
            return list(self.nodes.values())


REGISTRY = Registry(ttl_s=120)


def is_healthy(node: Dict[str, Any]) -> bool:
    #detect a failure in the secondary node by pulling its health endpoint
    #this detects failures in a node before running.
    try:
        with urllib.request.urlopen(f"http://{node['host']}:{node['port']}/health", timeout=1):
            return True
    except Exception:
        return False


def _post_json(url: str, payload: Dict[str, Any], timeout_s: int = 60) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def split_into_slices(low: int, high: int, n: int) -> List[Tuple[int, int]]:
    if n <= 0:
        return []
    total = high - low
    base = total // n
    rem = total % n
    out = []
    start = low
    for i in range(n):
        size = base + (1 if i < rem else 0)
        end = start + size
        if start < end:
            out.append((start, end))
        start = end
    return out


def distributed_compute(payload: Dict[str, Any]) -> Dict[str, Any]:
    low = int(payload["low"])
    high = int(payload["high"])
    if high <= low:
        raise ValueError("high must be > low")

    mode = str(payload.get("mode", "count"))
    if mode not in ("count", "list"):
        raise ValueError("mode must be 'count' or 'list'")
    
    sec_exec = str(payload.get("secondary_exec", "processes"))
    if sec_exec not in ("single", "threads", "processes"):
        raise ValueError("secondary_exec must be single|threads|processes")

    sec_workers = payload.get("secondary_workers", None)
    if sec_workers is not None:
        sec_workers = int(sec_workers)

    max_return_primes = int(payload.get("max_return_primes", 5000))
    include_per_node = bool(payload.get("include_per_node", False))

    nodes = REGISTRY.active_nodes()
    if not nodes:
        raise ValueError("no active secondary nodes registered")

    #potentially transient so just dont include it in the run for calculations.
    healthy_nodes = [n for n in nodes if is_healthy(n)]
    if not healthy_nodes:
        raise ValueError("no healthy secondary nodes available")
    
    chunk = int(payload.get("chunk", 500_000))

    nodes_sorted = sorted(healthy_nodes, key=lambda n: n["node_id"])
    slices = split_into_slices(low, high, len(nodes_sorted))
    nodes_sorted = nodes_sorted[:len(slices)]

    t0 = time.perf_counter()

    per_node_results: List[Dict[str, Any]] = []
    total_primes = 0
    primes_sample: List[int] = []
    primes_truncated = False
    max_prime = -1

    def call_node(node: Dict[str, Any], sl: Tuple[int, int]) -> Dict[str, Any]:
        host = node["host"]
        port = node["port"]
        url = f"http://{host}:{port}/compute"
        req = {
            "low": sl[0],
            "high": sl[1],
            "mode": mode,
            "chunk": chunk,
            "exec": sec_exec,
            "workers": sec_workers,
            "max_return_primes": max_return_primes if mode == "list" else 0,
            "include_per_chunk": False,
        }
        req = {k: v for k, v in req.items() if v is not None}

        t_call0 = time.perf_counter()
        resp = _post_json(url, req, timeout_s=3600)
        t_call1 = time.perf_counter()

        if not resp.get("ok"):
            raise RuntimeError(f"node {node['node_id']} error: {resp}")
        
        node_elapsed_s = float(resp.get("elapsed_seconds", 0.0))
        print(f"Node ID: {node['node_id']} completed in: {node_elapsed_s}")

        return {
            "node_id": node["node_id"],
            "node": {"host": host, "port": port, "cpu_count": node.get("cpu_count", 1)},
            "slice": list(sl),
            "round_trip_s": t_call1 - t_call0,
            "node_elapsed_s": node_elapsed_s,
            "node_sum_chunk_s": float(resp.get("sum_chunk_compute_seconds", 0.0)),
            "total_primes": int(resp.get("total_primes", 0)),
            "max_prime": int(resp.get("max_prime", -1)),
            "primes": resp.get("primes", None),
            "primes_truncated": bool(resp.get("primes_truncated", False)),
        }

    #also mitigating the node failures in the middle of work by retrying failed slices on surviving nodes.
    #to get the same forward progress. Simplisitic 1 time retry on a surviving node to not take too long.
    failed_slices = []
    surviving_nodes = []
    with ThreadPoolExecutor(max_workers=min(32, len(nodes_sorted))) as ex:
        future_to_task = {
            ex.submit(call_node, node, sl): (node, sl)
            for node, sl in zip(nodes_sorted, slices)
        }
        for f in as_completed(future_to_task):
            node, sl = future_to_task[f]
            try:
                per_node_results.append(f.result())
                surviving_nodes.append(node)
            except Exception as e:
                print(f"[primary_node] Node {node['node_id']} failed on slice {sl}: {e}")
                failed_slices.append(sl)

    if failed_slices:
        if not surviving_nodes:
            raise RuntimeError("All secondary nodes failed to compute their slices.")
        for i, sl in enumerate(failed_slices):
            backup_node = surviving_nodes[i % len(surviving_nodes)]
            print(f"[primary_node] Retrying slice {sl} on surviving node {backup_node['node_id']}...")
            per_node_results.append(call_node(backup_node, sl))

    per_node_results.sort(key=lambda r: r["slice"][0])

    for r in per_node_results:
        total_primes += int(r["total_primes"])
        max_prime = max(max_prime, int(r["max_prime"]))
        if mode == "list" and r.get("primes") is not None:
            ps = list(r["primes"])
            if len(primes_sample) < max_return_primes:
                remaining = max_return_primes - len(primes_sample)
                primes_sample.extend(ps[:remaining])
                if len(ps) > remaining:
                    primes_truncated = True
            else:
                primes_truncated = True
            if r.get("primes_truncated"):
                primes_truncated = True

    t1 = time.perf_counter()

    resp: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range": [low, high],
        "nodes_used": len(nodes_sorted),
        "secondary_exec": sec_exec,
        "secondary_workers": sec_workers,
        "chunk": chunk,
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_node_compute_seconds": sum(float(r["node_elapsed_s"]) for r in per_node_results),
        "sum_node_round_trip_seconds": sum(float(r["round_trip_s"]) for r in per_node_results),
    }

    if mode == "list":
        resp["primes"] = primes_sample
        resp["primes_truncated"] = primes_truncated
        resp["max_return_primes"] = max_return_primes

    if include_per_node:
        resp["per_node"] = per_node_results

    return resp


class Handler(BaseHTTPRequestHandler):
    server_version = "PrimaryPrimeCoordinator/1.0"

    def _send_json(self, obj: Dict[str, Any], code: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            return self._send_json({"ok": True, "status": "healthy"})
        if parsed.path == "/nodes":
            nodes = REGISTRY.active_nodes()
            nodes.sort(key=lambda n: n["node_id"])
            return self._send_json({"ok": True, "nodes": nodes, "ttl_s": REGISTRY.ttl_s})
        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            return self._send_json({"ok": False, "error": "invalid content-length"}, code=400)

        body = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except Exception as e:
            return self._send_json({"ok": False, "error": f"bad json: {e}"}, code=400)

        if parsed.path == "/register":
            for k in ("node_id", "host", "port"):
                if k not in payload:
                    return self._send_json({"ok": False, "error": f"missing field: {k}"}, code=400)
            rec = REGISTRY.upsert(payload)
            print(f"[primary_node] Added node: {payload} to registry")
            return self._send_json({"ok": True, "node": rec})

        if parsed.path == "/compute":
            try:
                for k in ("low", "high"):
                    if k not in payload:
                        raise ValueError(f"missing field: {k}")
                resp = distributed_compute(payload)
                return self._send_json(resp, code=200)
            except Exception as e:
                return self._send_json({"ok": False, "error": str(e)}, code=400)

        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def log_message(self, fmt, *args):
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9200)
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive (default 3600).")
    args = ap.parse_args()

    global REGISTRY
    REGISTRY = Registry(ttl_s=max(10, int(args.ttl)))

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[primary_node] listening on http://{args.host}:{args.port}")
    print("  GET  /health")
    print("  GET  /nodes")
    print("  POST /register")
    print("  POST /compute")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
    finally:
        httpd.server_close()
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()
