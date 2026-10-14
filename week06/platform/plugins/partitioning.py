"""Which node stores a key, and what AddNode / pin do to that map.

In the readings: partitioning / sharding — Think Distributed Systems ch. 7;
Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def owner(self, node: Node, key: bytes, mode: str) -> str:
        """Which node id should own ``key`` under this partitioning mode.

        HASH_MOD remaps most keys when membership changes; SLOTS moves about
        ``1/(N+1)``; KEY_RANGE keeps neighbors together; DIRECTORY honors pin;
        ITEM ignores pin. ``N`` is ``len(node.nodes)``.
        """
        raise NotImplementedError("partitioning.owner")

    def on_add_node(self, node: Node, request: node_pb2.AddNodeRequest) -> node_pb2.StatusResponse:
        """Handle ``AddNode`` / pin. Return a StatusResponse. Move bytes that changed owner."""
        raise NotImplementedError("partitioning.on_add_node")

    def rebalance(self, node: Node, modes: node_pb2.Modes) -> None:
        """Send keys this node holds to their current owner if that owner is someone else."""
        raise NotImplementedError("partitioning.rebalance")
