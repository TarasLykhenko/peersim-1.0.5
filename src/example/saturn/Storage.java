package example.saturn;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class Storage {

    Queue<Long> metaQueue = new LinkedList<>();
    HashMap<Long, Pair<Integer,Long>> pendingRemoteUpdates = new HashMap<>();
    HashMap<Integer, Long> kvStore = new HashMap<Integer, Long>();
    ReplicationManager replicationManager;

    public void Storage(ReplicationManager _replicationManager){
        replicationManager = _replicationManager;
    }

    public Long get(Integer key){
        return kvStore.get(key);
    }

    public void put(Integer key, Long value){
        kvStore.put(key, value);
        replicationManager.propagateUpdate(key, value);
    }

    public void remotePut(Long updateID, Integer key, Long value){
        pendingRemoteUpdates.put(updateID,new Pair<>(key, value));
        applyRemoteUpdates();
    }

    public void remoteMetadata(Long updateID){
        metaQueue.add(updateID);
        applyRemoteUpdates();
    }

    public void applyRemoteUpdates(){

        while(!metaQueue.isEmpty()){
            if(pendingRemoteUpdates.containsKey(metaQueue.peek())){
                Long updateID = metaQueue.poll();
                Pair<Integer,Long> update = pendingRemoteUpdates.get(updateID);
                pendingRemoteUpdates.remove(updateID);
                put(update.getKey(), update.getValue());
            }
            else{
                return;
            }
        }
    }


}
