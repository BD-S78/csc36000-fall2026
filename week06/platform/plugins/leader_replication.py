"""Who may accept a Write on this node, especially after a split.

In the readings: single-leader, multileader, leaderless — Think Distributed
Systems §8.4.5.

See week06/README.md. Useful imports: ``from platform.util import mode, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node

from .base import PluginBase


class Plugin(PluginBase):
    def admit(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """Drive the Write: accept here (return None), forward, or reject.

        ``None`` means this node continues and stores. A WriteResponse means
        the plugin already forwarded or failed (``not leader``, hop limit, …).
        """
        # Note: In SINGLE_LEADER, only node.leader admits writes (followers reject with "not leader").
        # In MULTI_LEADER and LEADERLESS, any node admits writes locally.
        raise NotImplementedError("leader_replication.admit")

    def after_local_write(
        self,
        node: Node,
        request: node_pb2.WriteRequest,
        value: bytes,
        owner: str,
    ) -> None:
        """After a successful local Write: MULTI_LEADER copies to other leaders."""
        # Architectural note on MULTI_LEADER:
        # The codebase only provides a single `node.leader` string (for SINGLE_LEADER)
        # and has no configuration to designate a mix of leaders and followers.
        # Because all nodes are treated as peer leaders in MULTI_LEADER, copying
        # to "other leaders" simply means replicating to all reachable peers in node.other_nodes().
        raise NotImplementedError("leader_replication.after_local_write")
