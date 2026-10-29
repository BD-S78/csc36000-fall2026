"""Write/Read that arrived on a node that does not own the key.

In the readings: request routing — Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, hop_count, mode

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
        routing = mode(request.modes, "request_routing")
        if not routing:
            # no --request-routing flag, keep the node default ("not owner")
            raise NotImplementedError("request_routing inactive")

        if routing == "CLIENT_AWARE":
            # the client has to resend to the owner, nothing is stored here
            return node_pb2.WriteResponse(ok=False, error="not owner", owner=owner)

        if hop_count(request) >= MAX_HOP:
            return node_pb2.WriteResponse(ok=False, error="hop limit", owner=owner)

        if routing == "FORWARD":
            resp = self.send(node, owner, "Write", request)
            if resp is None:
                return node_pb2.WriteResponse(ok=False, error="owner unreachable", owner=owner)
            return resp

        if routing == "DIRECTORY_TIER":
            # already routed once, don't route again
            # answer with the owner we know so the sender can fix its directory
            if hop_count(request) > 0:
                return node_pb2.WriteResponse(ok=False, error="not owner", owner=owner)

            target = node.directory.get(request.key, owner)
            resp = self.send(node, target, "Write", request)

            # stale entry: the node we looked up is down, or it says someone else owns the key
            # retry one time, with the owner from its answer or the owner we compute
            if resp is None or (not resp.ok and resp.error == "not owner"):
                retry = resp.owner if resp is not None and resp.owner else owner
                if retry != target:
                    target = retry
                    node.directory[request.key] = target
                    resp = self.send(node, target, "Write", request)

            if resp is None:
                return node_pb2.WriteResponse(ok=False, error="owner unreachable", owner=target)
            if resp.ok:
                node.directory[request.key] = resp.owner or target
            return resp

        return node_pb2.WriteResponse(ok=False, error=f"unknown mode {routing}", owner=owner)

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Same idea for Read. Return a ReadResponse or None."""
        routing = mode(request.modes, "request_routing")
        if not routing:
            raise NotImplementedError("request_routing inactive")

        # node.py calls this on the owner too, then it just reads locally
        if owner == node.node_id:
            return None

        if routing == "CLIENT_AWARE":
            return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)

        if hop_count(request) >= MAX_HOP:
            return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)

        if routing == "FORWARD":
            resp = self.send(node, owner, "Read", request)
            if resp is None:
                return node_pb2.ReadResponse(ok=False, error="owner unreachable", served_by=node.node_id)
            return resp

        if routing == "DIRECTORY_TIER":
            if hop_count(request) > 0:
                return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)

            target = node.directory.get(request.key, owner)
            resp = self.send(node, target, "Read", request)

            # stale entry: ReadResponse has no owner field,
            # so retry one time with the owner this node computes
            if (resp is None or (not resp.ok and resp.error == "not owner")) and target != owner:
                target = owner
                node.directory[request.key] = target
                resp = self.send(node, target, "Read", request)

            if resp is None:
                return node_pb2.ReadResponse(ok=False, error="owner unreachable", served_by=node.node_id)
            if resp.ok:
                node.directory[request.key] = target
            return resp

        return node_pb2.ReadResponse(ok=False, error=f"unknown mode {routing}", served_by=node.node_id)

    def send(self, node: Node, peer: str, method: str, request):
        """Send a copy of the request to peer with hop + 1. None if peer can't be reached."""
        try:
            return node.rpc(peer, method, copy_with_hop(request))
        except RpcFailed:
            return None
