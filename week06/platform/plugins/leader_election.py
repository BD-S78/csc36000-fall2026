"""Who is leader after a kill or a split: Bully, Ring, or Raft-style majority.

In the readings: leader election (Bully, ring, majority/term). These pick the
leader in Think Distributed Systems §8.4.5.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def on_elect(self, node: Node, request: node_pb2.ElectRequest) -> node_pb2.ElectResponse:
        """Start an election (empty candidate) or handle a vote / coordinator message.

        RAFT_STYLE majority is ``len(node.nodes)//2 + 1``.
        """
        raise NotImplementedError("leader_election.on_elect")
