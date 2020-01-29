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

        List<Long> remoteReplicas = getRemoeReplicasID(newUpdateId);
        for (long remoteReplicaID : remoteReplicas) {

            RemoteUpdateMessage message = new RemoteUpdateMessage(key,value,newUpdateId);
            message.setNodeDestinationID(remoteReplicaID);
            pendingSendUpdates.add(message);

        }

    }

    public Message getMessage(){
        return pendingSendUpdates.poll();
    }

    public List<Long> getRemoeReplicasID(long key){
        //TODO saber quais a replicas que replicam esta chave TARAS
        List<Long> list = new ArrayList<>();
        list.add(1L);
        list.add(2L);
        list.add(3L);
        return list;
    }
}
