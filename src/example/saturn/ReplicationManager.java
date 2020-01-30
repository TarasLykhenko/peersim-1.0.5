package example.saturn;

import example.saturn.datatypes.message.types.Message;
import example.saturn.datatypes.message.types.RemoteUpdateMessage;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationManager {

    Queue<RemoteUpdateMessage> pendingSendUpdates = new LinkedList<>();

    AtomicLong logicClock = new AtomicLong(0);
    Broker broker;
    long nodeID;


    public ReplicationManager(Broker _broker){
        broker = _broker;
    }

    public void setNodeID(long nodeId){
        this.nodeID = nodeId;
    }

    public void propagateUpdate(Integer key, Long value){

        Long newUpdateId = logicClock.getAndIncrement();
        broker.newUpdate(newUpdateId);

        List<Long> remoteReplicas = getRemoteReplicasID(key);

        for (long remoteReplicaID : remoteReplicas) {

            if(remoteReplicaID == this.nodeID) continue;

            RemoteUpdateMessage message = new RemoteUpdateMessage(key,value,newUpdateId);
            message.setNodeDestinationID(remoteReplicaID);
            pendingSendUpdates.add(message);

        }

    }

    public Message getMessage(){
        return pendingSendUpdates.poll();
    }

    public List<Long> getRemoteReplicasID(int key){
        return GlobalContext.keysToDcs.get(key);
    }
}
