"""What a copy on the wire looks like, how a node applies it, and how a lagging node catches up.

In the readings: state-based vs log-based replication — Think Distributed
Systems §8.4.4.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, encode_store, decode_store``.
"""
from __future__ import annotations
from platform.util import encode_store, mode, decode_store, RpcFailed
from platform import node_pb2
from platform.node import Node

from .base import PluginBase
from typing import List, Tuple

class Plugin(PluginBase):
    def install(
        self, node: Node, key: bytes, value: bytes, modes: node_pb2.Modes
    ) -> node_pb2.ReplicateRequest:
        """Apply a Write to local store/log and return the ReplicateRequest to send."""
        #We need to check the mode first
        replication_mode = mode(modes, "replication")
        if replication_mode not in ("STATE", "LOG", "LOG_WITH_SNAPSHOT"):
            raise ValueError(f"Unknown replication mode: {replication_mode}")
        with node.lock:
            #using the same code from node.py
            node.store[key]=value #this will be the default for every mode
            index= node.last_applied + 1 #our current index
            node.last_applied = index # this is the index of the last node
            node.log.append((index, key,value)) #we will still add to the log even if STATE or not
            if(modes.replication == "STATE"):
                return node_pb2.ReplicateRequest(
                    #creating the replicated node
                    key=key,
                    value=value,
                    kind="STATE",
                    index=index,
                    source=node.node_id,
                    snapshot_blob=encode_store(node.store),
                    modes=modes,
                )
            
            elif(modes.replication == "LOG"):
                return node_pb2.ReplicateRequest(
                    key=key,
                    value=value,
                    kind="LOG",
                    index=index,
                    source=node.node_id,
                    modes=modes,
                )
                
            else: # LOG_WITH_SNAPSHOT
                return node_pb2.ReplicateRequest(
                    key=key,
                    value=value,
                    kind="LOG_WITH_SNAPSHOT",
                    index=index,
                    source=node.node_id,
                    modes=modes,
                )

    def on_replicate(
        self, node: Node, request: node_pb2.ReplicateRequest
    ) -> node_pb2.ReplicateResponse:
        """Apply an incoming ReplicateRequest. Return ReplicateResponse."""
        index= int(request.index)
        with node.lock:
            #basically checks is there is a key  and snapshot_blob
            #we have to check the kind of replication
            if request.kind == "STATE":
                if request.snapshot_blob:
                    #clear everything for the new data
                    node.store.clear()
                    node.log.clear()
                    node.store.update(decode_store(request.snapshot_blob))
                node.last_applied=index
            
            elif request.kind == "LOG" or request.kind == "LOG_WITH_SNAPSHOT":
                #this scenario is if the replica is being update with the next log after it
                if index > node.last_applied + 1:
                    return node_pb2.ReplicateResponse(
                        ok=False, applied_index=node.last_applied,error="unknown kind"
                    )
                if index==node.last_applied + 1:
                    node.store[request.key]= request.value
                    node.log.append((index,request.key,request.value))
                    node.last_applied=index
            else:
                return node_pb2.ReplicateResponse(
                    ok=False, applied_index=node.last_applied,error="unknown kind"
                )    
            return node_pb2.ReplicateResponse(ok=True, applied_index=node.last_applied)
        
        #raise NotImplementedError("replication.on_replicate")
        


    def catch_up(self, node: Node, modes: node_pb2.Modes) -> None:
        """Push a log suffix, or a snapshot if a peer is far behind, using a per-peer index."""
        #first we need a way to keep track of the peers, to know which is behind of not
        if not hasattr(self,"recorders"):
            self.recorders={}
        replication_mode=mode(modes,"replication")
        #for all the modes, we will go through each node
        if replication_mode == "STATE":
            for peers in node.other_nodes():
                with node.lock:
                    last_node= node.last_applied
                    #checks if the current peer is behind, if not then move on
                    if self.recorders.get(peers,-1)>=last_node:
                        continue #means we already have it so move on to the next one
                    snapshot=encode_store(node.store)
                
                try:
                    #create a request
                    response=node.rpc(peers,"Replicate",node_pb2.ReplicateRequest(
                        kind="STATE",
                        index=last_node,
                        source= node.node_id,
                        snapshot_blob=snapshot,
                        modes=modes,
                    ))
                    # now we have all the information to add the progress to recorder
                    self.recorders[peers]=int(response.applied_index)
                except RpcFailed:
                    continue
        
        elif replication_mode=="LOG" or replication_mode== "LOG_WITH_SNAPSHOT":
            for peers in node.other_nodes():
                with node.lock:
                    last_node= node.last_applied
                    current=self.recorders.get(peers,-1)
                    #checks if the current peer is behind, if not then move on
                    if current>=last_node:
                        continue #means we already have it so move on to the next one
                    logs = [log for log in node.log if log[0] >current] # check for all the missing logs in the node.
                    snapshot=None
                    
                    if replication_mode=="LOG_WITH_SNAPSHOT":
                        bool_snapshot= False
                        if len(logs)> 2:
                            #if the logs missing is more than 8 indexes, then we need a snaphot
                            bool_snapshot=True
                        elif logs and logs[0][0] != current + 1:
                            bool_snapshot=True
                        #finally create the snapshot
                        if bool_snapshot:
                            snapshot=encode_store(node.store)
                
                try:
                    #create a request
                    if snapshot is not None:
                        response = node.rpc(peers, "Replicate", node_pb2.ReplicateRequest(
                            kind="STATE",
                            index=last_node,
                            source=node.node_id,
                            snapshot_blob=snapshot,
                            modes=modes,
                        ))
                        self.recorders[peers]=int(response.applied_index)
                    else:
                        for index, key, value in logs:
                            #go through every log
                            response=node.rpc(peers,"Replicate",node_pb2.ReplicateRequest(
                                key=key,
                                value=value,
                                kind="LOG",
                                index=index,
                                source= node.node_id,
                                snapshot_blob=snapshot,
                                modes=modes,
                            ))
                            # now we have all the information to add the progress to recorder
                            self.recorders[peers]=int(response.applied_index)
                            if not response.ok:
                                break
                except RpcFailed:
                    continue
  
