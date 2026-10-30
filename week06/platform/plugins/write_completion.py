"""When Write returns: send copies, wait for W, time out if a node is paused.

In the readings: synchronous / asynchronous / hybrid (quorum) replication —
Think Distributed Systems §8.4.3; Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

import threading
from collections import defaultdict, deque

from platform import node_pb2
from platform.node import Node
from platform.util import RpcFailed, mode

from .base import PluginBase


class Plugin(PluginBase):
    def __init__(self) -> None:
        self._cv = threading.Condition()
        self._pending = defaultdict(deque)
        self._working: set[str] = set()

    def _start_peer(self, node: Node, peer: str) -> None:
        if peer in self._working or not self._pending[peer]:
            return
        self._working.add(peer)
        threading.Thread(target=self._deliver, args=(node, peer), daemon=True).start()

    def _deliver(self, node: Node, peer: str) -> None:
        while True:
            with self._cv:
                if not self._pending[peer]:
                    self._working.remove(peer)
                    return
                request, acked = self._pending[peer][0]
            try:
                response = node.rpc(peer, "Replicate", request)
            except RpcFailed:
                response = None
            with self._cv:
                if response is None or not response.ok:
                    self._working.remove(peer)
                    self._cv.notify_all()
                    return
                self._pending[peer].popleft()
                acked.add(peer)
                self._cv.notify_all()

    def spread(
        self,
        node: Node,
        key: bytes,
        value: bytes,
        owner: str,
        modes: node_pb2.Modes,
    ) -> node_pb2.WriteResponse:
        """Install locally (via the replication plugin), send copies, wait for W acks.

        W comes from the current membership ``len(node.nodes)``: 1, majority
        (``N//2 + 1``), or all N. Return a WriteResponse: ok if W members acked,
        otherwise ``ack timeout``. Remember failed copies so ``repair`` / ``on_read``
        can finish an incomplete write after the peer is reachable again.
        """
        selected = mode(modes, "write_completion") or "ACK_ONE"
        if selected not in {"ACK_ONE", "ACK_QUORUM", "ACK_ALL"}:
            return node_pb2.WriteResponse(ok=False, error="unknown write completion mode", owner=owner)

        n = len(node.nodes)
        needed = {"ACK_ONE": 1, "ACK_QUORUM": n // 2 + 1, "ACK_ALL": n}[selected]
        try:
            request = node.plugins["replication"].install(node, key, value, modes)
        except NotImplementedError:
            if mode(modes, "replication") not in {"", "STATE"}:
                return node_pb2.WriteResponse(ok=False, error="replication mode unavailable", owner=owner)
            node._local_write(key, value)
            request = node_pb2.ReplicateRequest(
                key=key, value=value, kind="STATE", index=node.last_applied,
                source=node.node_id, modes=modes,
            )

        acked = {node.node_id}
        with self._cv:
            for peer in node.other_nodes():
                self._pending[peer].append((request, acked))
                self._start_peer(node, peer)
            self._cv.wait_for(lambda: len(acked) >= needed, timeout=3.0)
            complete = len(acked) >= needed
            return node_pb2.WriteResponse(
                ok=complete, error="" if complete else "ack timeout",
                owner=owner, acked_by=sorted(acked),
            )

    def repair(self, node: Node, modes: node_pb2.Modes) -> None:
        """Hinted handoff: retry copies that timed out while a peer was paused."""
        with self._cv:
            for peer in list(self._pending):
                self._start_peer(node, peer)

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Read repair: if this node is missing ``key``, pull from a peer and store it."""
        if request.hop:
            return None
        with node.lock:
            if request.key in node.store:
                return None
        for peer in dict.fromkeys([owner, *node.other_nodes()]):
            if peer == node.node_id:
                continue
            probe = node_pb2.ReadRequest(key=request.key, modes=request.modes, hop=1)
            try:
                response = node.rpc(peer, "Read", probe)
            except RpcFailed:
                continue
            if response.ok and response.found:
                with node.lock:
                    current = node.store.get(request.key)
                if current is None:
                    node._local_write(request.key, response.value)
                    current = response.value
                return node_pb2.ReadResponse(
                    ok=True, found=True, value=current, served_by=node.node_id,
                )
        return None
