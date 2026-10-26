"""Multi-step runs: skip vs retry vs exactly-once, resume from log or snapshot.

In the readings: at-most-once, at-least-once, exactly-once processing —
Think Distributed Systems ch. 4.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def start(self, node: Node, request: node_pb2.RunStartRequest) -> node_pb2.RunResponse:
        """RunStart: execute steps under at-most / at-least / exactly-once.

        Keep a durable run record on ``node.runs`` so ``resume`` can continue
        after this node was paused.
        """
        raise NotImplementedError("processing_guarantees.start")

    def resume(self, node: Node, request: node_pb2.RunResumeRequest) -> node_pb2.RunResponse:
        """RunResume from a log position or from a snapshot."""
        raise NotImplementedError("processing_guarantees.resume")

    def get(self, node: Node, request: node_pb2.RunGetRequest) -> node_pb2.RunResponse:
        raise NotImplementedError("processing_guarantees.get")
