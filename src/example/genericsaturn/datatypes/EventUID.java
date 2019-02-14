package example.genericsaturn.datatypes;

import java.util.UUID;

public class EventUID {

    private int epoch;
    private int timestamp;
    private long src;
    private long dst;
    private boolean isMigration = false;
    private long migrationTarget;

    private Operation operation;
    private UUID identifier;

    public Operation getOperation() {
        return operation;
    }

    public EventUID(Operation operation, int timestamp, int epoch, long src, long dst) {
        this.operation = operation;
        this.timestamp = timestamp;
        this.epoch = epoch;
        this.src = src;
        this.dst = dst;
    }

    public long getMigrationTarget() {
        return migrationTarget;
    }

    public boolean isMigration() {
        return isMigration;
    }

    public void setMigration(boolean bool, long targetId) {
        migrationTarget = targetId;
        isMigration = bool;
        identifier = UUID.randomUUID();
    }

    public UUID getIdentifier() {
        return identifier;
    }

    public long getSrc() {
        return src;
    }

    public void setSrc(long src) {
        this.src = src;
    }

    public long getDst() {
        return dst;
    }

    public void setDst(long dst) {
        this.dst = dst;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public String toString() {
        return "<" + operation.getKey() + ", " + timestamp + ">";
    }

    public String toStringFileFormat() {
        return operation.getKey() + "," + timestamp;
    }

    public EventUID clone() {
        return new EventUID(new Operation(operation.getType(), operation.getKey()),
                timestamp,
                epoch,
                src,
                dst);
    }

}
