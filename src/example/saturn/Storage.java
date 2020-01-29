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

    public Storage(ReplicationManager _replicationManager){
        replicationManager = _replicationManager;
    }

    public long get(int key){
        return kvStore.get(key);
    }

    public void put(int key, long value){
        kvStore.put(key, value);
        replicationManager.propagateUpdate(key, value);
    }

    public void remotePut(long updateID, int key, long value){
        pendingRemoteUpdates.put(updateID,new Pair<>(key, value));
        applyRemoteUpdates();
    }

    public void remoteMetadata(long updateID){
        metaQueue.add(updateID);
        applyRemoteUpdates();
    }

    public void applyRemoteUpdates(){

        while(!metaQueue.isEmpty()){
            if(pendingRemoteUpdates.containsKey(metaQueue.peek())){
                long updateID = metaQueue.poll();
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
