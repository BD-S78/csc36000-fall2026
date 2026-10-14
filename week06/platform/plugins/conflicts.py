"""Combine two values for the same key into one value.

In the readings: operation-based CRDT, state-based CRDT, application
reconciliation — Think Distributed Systems §9.3.3.

See week06/README.md. Useful imports: ``from platform.util import encode_meta, decode_meta``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def combine(self, left: bytes, right: bytes, mode: str) -> bytes:
        """Two blobs for one key → one blob."""
        raise NotImplementedError("conflicts.combine")

    def on_write(self, node: Node, key: bytes, incoming: bytes, mode: str) -> bytes:
        """If ``key`` already has bytes, combine; otherwise return ``incoming``."""
        raise NotImplementedError("conflicts.on_write")

    def on_replicate(
        self, node: Node, request: node_pb2.ReplicateRequest
    ) -> node_pb2.ReplicateResponse:
        """A copy arrived. Merge with what this node already has (do not blindly overwrite)."""
        raise NotImplementedError("conflicts.on_replicate")

    def heal(self, node: Node, modes: node_pb2.Modes) -> None:
        """After a split heals: exchange values with peers so both sides converge."""
        raise NotImplementedError("conflicts.heal")

    def export_meta(self, node: Node, key: bytes, value: bytes, mode: str) -> bytes:
        """Metadata to attach on Replicate (op ids, version vector, timestamp)."""
        raise NotImplementedError("conflicts.export_meta")
