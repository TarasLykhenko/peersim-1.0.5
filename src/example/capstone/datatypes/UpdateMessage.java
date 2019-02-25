package example.capstone.datatypes;

import java.util.HashMap;
import java.util.Map;

public class UpdateMessage {

    private final long originalDC;
    private final int key;
    private Map<Long, Integer> vectorClock;
    private long lastSender;

    public Map<Long, Integer> getVectorClock() {
        return vectorClock;
    }

    public UpdateMessage(long originalDC, int key, Map<Long, Integer> localDataClock, long sender) {
        this.originalDC = originalDC;
        this.key = key;
        this.vectorClock = new HashMap<>(localDataClock);
        this.lastSender = sender;
    }

    public UpdateMessage(UpdateMessage updateMessage, long lastSender) {
        this.originalDC = updateMessage.originalDC;
        this.key = updateMessage.key;
        this.vectorClock = new HashMap<>(updateMessage.vectorClock);
        this.lastSender = lastSender;
    }

    public void updateVectorClockEntry(long nodeId, int value) {
        lastSender = nodeId;
        int beforeSize = vectorClock.size();
        vectorClock.put(nodeId, value);

        if (beforeSize != vectorClock.size()) {
            throw new RuntimeException("Vector clock size should not increase.");
        }
    }

    public long getOriginalDC() {
        return this.originalDC;

    }

    public int getKey() {
        return this.key;
    }

    public long getLastSender() {
        return lastSender;
    }
}
