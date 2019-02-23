package example.common.datatypes;

import peersim.core.CommonState;

public class EventUID {

    private long src;
    private long dst;
    private Operation operation;
    private long timestamp;


    public EventUID(long src, long dst, Operation operation) {
        if (operation == null) {
            System.out.println("WTF!");
        }
        this.operation = operation;
        this.timestamp = CommonState.getTime();
        this.src = src;
        this.dst = dst;
    }

    public EventUID(EventUID event) {
        this.operation = event.operation;
        this.timestamp = event.timestamp;
        this.src = event.src;
        this.dst = event.dst;
    }

    public Operation getOperation() {
        return operation;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public String toString() {
        return "<" + operation.getKey() + ", " + timestamp + ">";
    }

    public String toStringFileFormat() {
        return operation.getKey() + "," + timestamp;
    }

}
