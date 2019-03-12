package example.capstonematrix.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadResult {

    private List<Map<Long, Integer>> hierarchicalRegionalClock;

    public ReadResult(List<Map<Long, Integer>> hierarchicalRegionalClock) {
        this.hierarchicalRegionalClock = new ArrayList<>(hierarchicalRegionalClock);
    }

    public List<Map<Long, Integer>> getHRC() {
        return hierarchicalRegionalClock;
    }
}
