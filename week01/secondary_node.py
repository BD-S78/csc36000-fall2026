#!/usr/bin/env python3
"""
secondary_node.py

A "secondary node" HTTP server that exposes prime-range computation via HTTP.

Key features
------------
- Exposes POST /compute with the same partitioning + thread/process execution model as primes_cli.py
- On startup, optionally registers itself with a primary coordinator (primary_node.py) via POST /register
  so the primary can discover and distribute work across all secondary nodes.

Endpoints
---------
GET  /health
    -> {"ok": true, "status": "healthy"}

GET  /info
    -> basic node metadata (host/port/node_id/cpu_count)

POST /compute
    JSON body:
    {
      "low": 0,                   (required)
      "high": 1000000,            (required; exclusive)
      "mode": "count"|"list",     default "count"
      "chunk": 500000,            default 500000
      "exec": "single"|"threads"|"processes", default "single"
      "workers": 8,               default cpu_count
      "max_return_primes": 5000,  default 5000 (only used when mode="list")
      "include_per_chunk": true   default false (summary only; avoids huge responses)
    }

Notes
-----
- Example of how to run from terminal: python3 week01/secondary_node.py --primary http://127.0.0.1:9200 --node-id kbrown
- For classroom demos, use mode="count" for big ranges to avoid large payloads.
- "threads" may not speed up CPU-bound work in CPython; "processes" usually will.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
from primes_in_range import get_primes


# ----------------------------
# Partitioning helpers
# ----------------------------

def iter_ranges(low: int, high: int, chunk: int) -> List[Tuple[int, int]]:
    """Split [low, high) into contiguous chunks."""
    if chunk <= 0:
        raise ValueError("chunk must be > 0")
    out: List[Tuple[int, int]] = []
    x = low
    while x < high:
        y = min(x + chunk, high)
        out.append((x, y))
        x = y
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Dict[str, Any]:
    """
    Worker for one chunk.
    Returns dict for easy JSON serialization.
    """
    low, high, return_list = args

    t0 = time.perf_counter()
    res = get_primes(low, high, return_list=return_list)
    t1 = time.perf_counter()

    if return_list:
        primes = list(res)  # type: ignore[arg-type]
        return {
            "low": low,
            "high": high,
            "elapsed_s": t1 - t0,
            "prime_count": len(primes),
            "max_prime": primes[-1] if primes else -1,
            "primes": primes,
        }

    count = int(res)  # type: ignore[arg-type]
    return {
        "low": low,
        "high": high,
        "elapsed_s": t1 - t0,
        "prime_count": count,
        "max_prime": -1,  # not computed in count mode to avoid extra work
    }


def compute_partitioned(
    low: int,
    high: int,
    *,
    mode: str = "count",
    chunk: int = 500_000,
    exec_mode: str = "single",
    workers: int | None = None,
    max_return_primes: int = 5000,
    include_per_chunk: bool = False,
) -> Dict[str, Any]:
    """
    Perform partitioned computation over [low, high) using get_primes per chunk.
    """
    if high <= low:
        raise ValueError("high must be > low")
    if mode not in ("count", "list"):
        raise ValueError("mode must be 'count' or 'list'")
    if exec_mode not in ("single", "threads", "processes"):
        raise ValueError("exec must be single|threads|processes")

    if workers is None:
        workers = os.cpu_count() or 4
    workers = max(1, int(workers))

    ranges = iter_ranges(low, high, chunk)
    want_list = (mode == "list")

    t0 = time.perf_counter()
    chunk_results: List[Dict[str, Any]] = []

    if exec_mode == "single":
        for a, b in ranges:
            chunk_results.append(_work_chunk((a, b, want_list)))

    elif exec_mode == "threads":
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())

    else:  # processes
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())

    t1 = time.perf_counter()

    chunk_results.sort(key=lambda d: int(d["low"]))
    total_primes = sum(int(d["prime_count"]) for d in chunk_results)
    sum_chunk = sum(float(d["elapsed_s"]) for d in chunk_results)

    primes_out: List[int] | None = None
    truncated = False
    max_prime = -1

    if want_list:
        primes_out = []
        for d in chunk_results:
            ps = d.get("primes") or []
            if ps:
                max_prime = max(max_prime, int(ps[-1]))
            if len(primes_out) < max_return_primes:
                remaining = max_return_primes - len(primes_out)
                primes_out.extend(ps[:remaining])
                if len(ps) > remaining:
                    truncated = True
            else:
                truncated = True

    response: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range": [low, high],
        "chunk": chunk,
        "exec": exec_mode,
        "workers": workers if exec_mode != "single" else 1,
        "chunks": len(ranges),
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_chunk_compute_seconds": sum_chunk,
    }

    if include_per_chunk:
        slim = []
        for d in chunk_results:
            slim.append({
                "low": d["low"],
                "high": d["high"],
                "elapsed_s": d["elapsed_s"],
                "prime_count": d["prime_count"],
                "max_prime": d.get("max_prime", -1),
            })
        response["per_chunk"] = slim

    if primes_out is not None:
        response["primes"] = primes_out
        response["primes_truncated"] = truncated
        response["max_return_primes"] = max_return_primes

    return response


# ----------------------------
# Registration with primary
# ----------------------------

def _guess_local_ip_for(primary_url: str) -> str:
    """
    Best-effort: pick the local IP used to reach the primary.
    Works well in a LAN lab environment.
    """
    try:
        u = urlparse(primary_url)
        host = u.hostname or "127.0.0.1"
        port = u.port or (443 if u.scheme == "https" else 80)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def _post_json(url: str, payload: Dict[str, Any], timeout_s: int = 5) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def start_registration_loop(
    primary_url: str,
    node_id: str,
    host: str,
    port: int,
    *,
    interval_s: int = 5,
) -> None:
    """
    Background heartbeat: periodically re-register so primary can expire stale nodes.
    """
    reg_url = primary_url.rstrip("/") + "/heartbeat"
    payload = {
        "node_id": node_id,
        "host": host,
        "port": port,
        "cpu_count": os.cpu_count() or 1,
        "ts": time.time(),
    }

    def loop():
        while True:
            payload["ts"] = time.time()
            try:
                _post_json(reg_url, payload, timeout_s=5)
            except Exception as e:
                # Ignore transient network failures; primary may be down temporarily.
                print(f"error when heartbeating node to primary: {e}")
                pass
            time.sleep(interval_s)

    th = threading.Thread(target=loop, daemon=True)
    th.start()


def send_unregister(primary_url: str, node_id: str, host: str, port: int) -> None:
    """Send a final graceful shutdown signal so the primary can remove this node immediately."""
    if not primary_url:
        return
    url = primary_url.rstrip("/") + "/unregister"
    payload = {
        "node_id": node_id,
        "host": host,
        "port": port,
        "ts": time.time(),
    }
    try:
        _post_json(url, payload, timeout_s=5)
    except Exception as e:
        print(f"warning: graceful unregister failed: {e}")


# ----------------------------
# HTTP server
# ----------------------------

NODE_META: Dict[str, Any] = {}


class Handler(BaseHTTPRequestHandler):
    server_version = "SecondaryPrimeNode/2.0"

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
        if parsed.path == "/info":
            return self._send_json({"ok": True, "node": NODE_META})
        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/compute":
            return self._send_json({"ok": False, "error": "not found"}, code=404)

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            return self._send_json({"ok": False, "error": "invalid content-length"}, code=400)

        if length <= 0:
            return self._send_json({"ok": False, "error": "empty body"}, code=400)

        if length > 10 * 1024 * 1024:
            return self._send_json({"ok": False, "error": "request too large"}, code=413)

        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as e:
            return self._send_json({"ok": False, "error": f"bad json: {e}"}, code=400)

        try:
            low = int(payload["low"])
            high = int(payload["high"])
        except Exception:
            return self._send_json({"ok": False, "error": "payload must include integer low and high"}, code=400)

        mode = str(payload.get("mode", "count"))
        chunk = int(payload.get("chunk", 500_000))
        exec_mode = str(payload.get("exec", "single"))
        workers = payload.get("workers", None)
        if workers is not None:
            workers = int(workers)
        max_return_primes = int(payload.get("max_return_primes", 5000))
        include_per_chunk = bool(payload.get("include_per_chunk", False))

        try:
            resp = compute_partitioned(
                low, high,
                mode=mode,
                chunk=chunk,
                exec_mode=exec_mode,
                workers=workers,
                max_return_primes=max_return_primes,
                include_per_chunk=include_per_chunk,
            )
            resp["node_id"] = NODE_META.get("node_id")
            return self._send_json(resp, code=200)
        except Exception as e:
            return self._send_json({"ok": False, "error": str(e)}, code=400)

    def log_message(self, fmt, *args):
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Secondary prime worker node (HTTP server).")
    ap.add_argument("--host", default="127.0.0.1", help="Bind host (default 127.0.0.1).")
    ap.add_argument("--port", type=int, default=9100, help="Bind port (default 9100).")
    ap.add_argument("--node-id", default=None, help="Optional stable node id (default: hostname).")

    ap.add_argument("--primary", default=None, help="Primary coordinator URL, e.g. http://134.74.160.1:9200")
    ap.add_argument("--public-host", default=None, help="Host/IP to advertise to primary (default: auto-detect).")
    ap.add_argument("--register-interval", type=int, default=5, help="Seconds between heartbeats (default 5).")

    args = ap.parse_args()

    node_id = args.node_id or os.uname().nodename

    advertised_host = args.public_host
    if args.primary and not advertised_host:
        advertised_host = _guess_local_ip_for(args.primary)
    if not advertised_host:
        advertised_host = "127.0.0.1"

    NODE_META.update({
        "node_id": node_id,
        "bind_host": args.host,
        "bind_port": args.port,
        "advertised_host": advertised_host,
        "advertised_port": args.port,
        "cpu_count": os.cpu_count() or 1,
        "registered_to": args.primary,
    })

    if args.primary:
        start_registration_loop(
            args.primary,
            node_id=node_id,
            host=advertised_host,
            port=args.port,
            interval_s=max(5, int(args.register_interval)),
        )

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[secondary_node] node_id={node_id}")
    print(f"[secondary_node] listening on http://{args.host}:{args.port}")
    print(f"[secondary_node] advertised as http://{advertised_host}:{args.port}")
    if args.primary:
        print(f"[secondary_node] registering to primary: {args.primary}")
    print("  GET  /health")
    print("  GET  /info")
    print("  POST /compute")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        if args.primary:
            send_unregister(args.primary, node_id, advertised_host, args.port)
        print("\n[secondary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
    finally:
        httpd.server_close()
        print("[secondary_node] server stopped.")


if __name__ == "__main__":
    main()
