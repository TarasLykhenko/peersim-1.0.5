package example.oldsaturn.datatypes;

import java.util.UUID;

public class EventUID {

    long key;
    int epoch;
    int timestamp;
    boolean remoteRead;
    long latency;
    long src;
    long dst;
    boolean isMigration = false;
    long migrationTarget;
    UUID identifier;

    public EventUID(long key, int timestamp, int epoch, boolean remoteRead, long lat, long src, long dst) {
        this.key = key;
        this.timestamp = timestamp;
        this.epoch = epoch;
        this.remoteRead = remoteRead;
        this.latency = lat;
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

    public long getLatency() {
        return latency;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
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


    public boolean getRemoteRead() {
        return remoteRead;
    }

    public void setRemoteRead(boolean remoteRead) {
        this.remoteRead = remoteRead;
    }

    public String toString() {
        return "<" + key + ", " + timestamp + ">";
    }

    public String toStringFileFormat() {
        return key + "," + timestamp;
    }

    public EventUID clone() {
        return new EventUID(key, timestamp, epoch, remoteRead, latency, src, dst);
    }

}
