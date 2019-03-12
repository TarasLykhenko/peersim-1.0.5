package example.myproject.datatypes;

import javafx.util.Pair;

public class MetadataEntry {

    private Pair<Long, Integer> entry;

    public MetadataEntry(long pathId, int value) {
        this.entry = new Pair<>(pathId, value);
    }

    public long getPathId() {
        return entry.getKey();
    }

    public int getValue() {
        return entry.getValue();
    }

}
