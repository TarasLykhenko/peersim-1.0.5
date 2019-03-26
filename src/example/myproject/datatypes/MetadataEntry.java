package example.myproject.datatypes;

import example.myproject.Initialization;
import javafx.util.Pair;

import java.util.Objects;

public class MetadataEntry {

    public enum State {
        OK,
        JUMP,
    }

    private Pair<Long, Integer> entry;
    private State state;

    public MetadataEntry(long pathId, int value) {
        this.entry = new Pair<>(pathId, value);
        this.state = State.OK;
    }

    public MetadataEntry() {
        this.entry = new Pair<>(-1L, -1);
        this.state = State.JUMP;
    }

    public long getPathId() {
        if (state == State.JUMP) {
            throw new AssertException("Attempting to get PathId of a Jump.");
        }

        return entry.getKey();
    }

    public int getValue() {
        if (state == State.JUMP) {
            throw new AssertException("Attempting to get Value of a Jump.");
        }

        return entry.getValue();
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        if (state == State.OK) {
            return Initialization.pathsToPathLongs.get(entry.getKey()) + " - " + entry.getValue();
        } else if (state == State.JUMP) {
            return "jump";
        } else {
            throw new AssertException("Unknown entry state: " + state);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetadataEntry that = (MetadataEntry) o;
        return Objects.equals(entry, that.entry) &&
                state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entry, state);
    }
}
