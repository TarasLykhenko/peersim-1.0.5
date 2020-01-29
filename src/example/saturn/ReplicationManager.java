package example.saturn;

import example.saturn.datatypes.message.types.Message;
import example.saturn.datatypes.message.types.RemoteUpdateMessage;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationManager {

    Queue<RemoteUpdateMessage> pendingSendUpdates = new LinkedList<>();

    AtomicLong logicClock = new AtomicLong(0);
    Broker broker;

    public ReplicationManager(Broker _broker){
        broker = _broker;
    }

    public void propagateUpdate(Integer key, Long value){

        Long newUpdateId = logicClock.getAndIncrement();
        broker.newUpdate(newUpdateId);

        List<Long> remoteReplicas = getRemoeReplicasID(key);

        for (long remoteReplicaID : remoteReplicas) {
            RemoteUpdateMessage message = new RemoteUpdateMessage(key,value,newUpdateId);
            message.setNodeDestinationID(remoteReplicaID);
            pendingSendUpdates.add(message);

        }

    }

    public Message getMessage(){
        return pendingSendUpdates.poll();
    }

    public List<Long> getRemoeReplicasID(int key){
        return GlobalContext.keysToDcs.get(key);
    }
}
