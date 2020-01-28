package example.saturn;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationManager {

    HashMap<Long, Pair<Integer,Long>> pendingSendUpdates = new HashMap<>();
    AtomicLong logicClock = new AtomicLong(0);
    Broker broker;

    public ReplicationManager(Broker _broker){
        broker = _broker;
    }

    public void propagateUpdate(Integer key, Long value){
        Long newUpdateId = logicClock.getAndIncrement();
        broker.newUpdate(newUpdateId);
        pendingSendUpdates.put(newUpdateId, new Pair<>(key, value));
    }

    public HashMap<Long, Pair<Integer,Long>> get
}
