package example.capstonematrix.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateMessage {

    private final long originalDC;
    private final int key;
    private List<Map<Long, Integer>> past;

    public UpdateMessage(long originalDC, int key, List<Map<Long, Integer>> past) {
        this.originalDC = originalDC;
        this.key = key;
        this.past = new ArrayList<>(past);
    }

    public long getOriginalDC() {
        return this.originalDC;

    }

    public int getKey() {
        return this.key;
    }

    public List<Map<Long, Integer>> getPast() {
        return past;
    }
}
