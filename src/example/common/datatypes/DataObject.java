package example.common.datatypes;

import java.util.Set;
import java.util.stream.Collectors;

public class DataObject {

    private int level;
    private Set<Long> nodeIDs;
    private int groupCounter;
    private int key;

    private String debugInfo;

    public DataObject(int level, Set<Long> nodeIDs, int groupCounter, int totalCounter) {
        this.level = level;
        this.nodeIDs = nodeIDs;
        this.groupCounter = groupCounter;
        this.key = totalCounter;
        this.debugInfo = "lvl: " + level + " | nodes:  " + nodeIDs.stream()
                .map(Object::toString)
                .collect(Collectors.joining("-"))
                + " | group number: " + groupCounter + " | unique number: " + totalCounter;
    }

    public int getLevel() {
        return level;
    }

    public int getGroupCounter() {
        return groupCounter;
    }

    public Set<Long> getNodeIDs() {
        return nodeIDs;
    }

    public String getDebugInfo() {
        return debugInfo;
    }

    public int getKey() {
        return key;
    }
}
