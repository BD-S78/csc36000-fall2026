"""Two-key work: 2PC, saga compensation, or refuse.

In the readings: atomic commit — Think Distributed Systems ch. 6 (2PC);
saga compensation (week 2 additional reading).

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def run(self, node: Node, request: node_pb2.TxnRequest) -> node_pb2.TxnResponse:
        """Drive TWO_PC, SAGA, or SINGLE_KEY_ONLY. Return a TxnResponse.

        TWO_PC prepare goes to the owner of each key. If a participant is
        paused, the txn does not commit. SAGA retries compensation if the
        restore Write fails.
        """
        raise NotImplementedError("atomic_commit.run")

    def on_prepare(
        self, node: Node, request: node_pb2.PrepareRequest
    ) -> node_pb2.PrepareResponse:
        raise NotImplementedError("atomic_commit.on_prepare")

    def on_commit(self, node: Node, request: node_pb2.CommitRequest) -> node_pb2.AckResponse:
        raise NotImplementedError("atomic_commit.on_commit")

    def on_abort(self, node: Node, request: node_pb2.AbortRequest) -> node_pb2.AckResponse:
        raise NotImplementedError("atomic_commit.on_abort")
