"""When Write returns: send copies, wait for W, time out if a node is paused.

In the readings: synchronous / asynchronous / hybrid (quorum) replication —
Think Distributed Systems §8.4.3; Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
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
        raise NotImplementedError("write_completion.spread")

    def repair(self, node: Node, modes: node_pb2.Modes) -> None:
        """Hinted handoff: retry copies that timed out while a peer was paused."""
        raise NotImplementedError("write_completion.repair")

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Read repair: if this node is missing ``key``, pull from a peer and store it."""
        raise NotImplementedError("write_completion.on_read")
