"""Who may accept a Write on this node, especially after a split.

In the readings: single-leader, multileader, leaderless — Think Distributed
Systems §8.4.5.

See week06/README.md. Useful imports: ``from platform.util import mode, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, hop_count, mode

from .base import PluginBase


class Plugin(PluginBase):
    def admit(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """Drive the Write: accept here (return None), forward, or reject.

        ``None`` means this node continues and stores. A WriteResponse means
        the plugin already forwarded or failed (``not leader``, hop limit, …).
        """
        rep_mode = mode(request.modes, "leader_replication")
        if not rep_mode:
            # Step out of the way so request_routing and other plugins can run
            raise NotImplementedError("leader_replication inactive")

        if rep_mode == "SINGLE_LEADER":
            if node.node_id == node.leader:
                return None

            if hop_count(request) >= MAX_HOP:
                return node_pb2.WriteResponse(
                    ok=False, error="hop limit", owner=node.leader
                )

            fwd = copy_with_hop(request)
            try:
                return node.rpc(node.leader, "Write", fwd)
            except RpcFailed:
                # Leader unreachable or isolated by partition
                return node_pb2.WriteResponse(
                    ok=False, error="not leader", owner=node.leader
                )

        if rep_mode in ("LEADERLESS", "MULTI_LEADER"):
            # Any node admits and writes locally
            return None

        return node_pb2.WriteResponse(
            ok=False, error=f"unknown mode {rep_mode}", owner=node.leader
        )

    def after_local_write(
        self,
        node: Node,
        request: node_pb2.WriteRequest,
        value: bytes,
        owner: str,
    ) -> None:
        """After a successful local Write: MULTI_LEADER copies to other leaders."""
        rep_mode = mode(request.modes, "leader_replication")
        if rep_mode != "MULTI_LEADER":
            return

        # Architectural note on MULTI_LEADER:
        # The codebase only provides a single `node.leader` string (for SINGLE_LEADER)
        # and has no configuration to designate a mix of leaders and followers.
        # Because all nodes are treated as peer leaders in MULTI_LEADER, copying
        # to "other leaders" simply means replicating to all reachable peers in node.other_nodes().
        for peer in node.other_nodes():
            try:
                node.rpc(
                    peer,
                    "Replicate",
                    node_pb2.ReplicateRequest(
                        key=request.key,
                        value=value,
                        index=-1,
                        source=node.node_id,
                        modes=request.modes,
                    ),
                )
            except RpcFailed:
                # Peer unreachable or partition dropped the call
                pass
