"""Rules for two Reads after a Write on the same session.

In the readings: session models (read-own-writes, monotonic reads) —
Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def on_write(self, node: Node, request: node_pb2.WriteRequest, value: bytes) -> None:
        """Record a per-session sequence (not just the last bytes) for this Write."""
        raise NotImplementedError("session_models.on_write")

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """READ_OWN_WRITES may wait or pull from a peer; MONOTONIC_READS refuse a lower seq."""
        raise NotImplementedError("session_models.on_read")
