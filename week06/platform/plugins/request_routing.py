"""Write/Read that arrived on a node that does not own the key.

In the readings: request routing — Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def on_write(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """If this node should not store the Write, forward, look up, or reject.

        Return a WriteResponse, or None to mean “handle it here.” Increment
        ``request.hop`` when forwarding; stop after 8. A stale directory lookup
        should retry once rather than drop the bytes.
        """
        raise NotImplementedError("request_routing.on_write")

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Same idea for Read. Return a ReadResponse or None."""
        raise NotImplementedError("request_routing.on_read")
