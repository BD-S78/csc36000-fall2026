"""What a copy on the wire looks like, how a node applies it, and how a lagging node catches up.

In the readings: state-based vs log-based replication — Think Distributed
Systems §8.4.4.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, encode_store, decode_store``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def install(
        self, node: Node, key: bytes, value: bytes, modes: node_pb2.Modes
    ) -> node_pb2.ReplicateRequest:
        """Apply a Write to local store/log and return the ReplicateRequest to send."""
        raise NotImplementedError("replication.install")

    def on_replicate(
        self, node: Node, request: node_pb2.ReplicateRequest
    ) -> node_pb2.ReplicateResponse:
        """Apply an incoming ReplicateRequest. Return ReplicateResponse."""
        raise NotImplementedError("replication.on_replicate")

    def catch_up(self, node: Node, modes: node_pb2.Modes) -> None:
        """Push a log suffix, or a snapshot if a peer is far behind, using a per-peer index."""
        raise NotImplementedError("replication.catch_up")
