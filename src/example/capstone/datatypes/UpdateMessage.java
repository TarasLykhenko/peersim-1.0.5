package example.capstone.datatypes;

import java.util.HashMap;
import java.util.Map;

public class UpdateMessage {

    private final int originalDC;
    private final int key;
    private Map<Integer, Integer> vectorClock;
    private int lastSender;

    public Map<Integer, Integer> getVectorClock() {
        return vectorClock;
    }

    public UpdateMessage(int originalDC, int key, Map<Integer, Integer> localDataClock, int sender) {
        this.originalDC = originalDC;
        this.key = key;
        this.vectorClock = new HashMap<>(localDataClock);
        this.lastSender = sender;
    }

    public UpdateMessage(UpdateMessage updateMessage, int lastSender) {
        this.originalDC = updateMessage.originalDC;
        this.key = updateMessage.key;
        this.vectorClock = new HashMap<>(updateMessage.vectorClock);
        this.lastSender = lastSender;
    }

    public void updateVectorClockEntry(int nodeId, int value) {
        lastSender = nodeId;
        int beforeSize = vectorClock.size();
        vectorClock.put(nodeId, value);

        if (beforeSize != vectorClock.size()) {
            throw new RuntimeException("Vector clock size should not increase.");
        }
    }

    public int getOriginalDC() {
        return this.originalDC;

    }

    public int getKey() {
        return this.key;
    }

    public int getLastSender() {
        return lastSender;
    }
}
