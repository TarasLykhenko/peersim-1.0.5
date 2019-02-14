package example.genericsaturn.datatypes;

import java.util.Set;
import java.util.stream.Collectors;

public class DataObject {

    private int level;
    private Set<Long> nodeIDs;
    private int groupCounter;
    private int totalCounter;

    private String uniqueId;
    public DataObject(int level, Set<Long> nodeIDs, int groupCounter, int totalCounter) {
        this.level = level;
        this.nodeIDs = nodeIDs;
        this.groupCounter = groupCounter;
        this.totalCounter = totalCounter;
        uniqueId = "lvl: " + level + " | nodes:  " + nodeIDs.stream()
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

    public String getUniqueId() {
        return uniqueId;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public int getTotalCounter() {
        return totalCounter;
    }
}
