package example.capstonematrix.datatypes;

public class UpdateMessage {

    private final long originalDC;
    private final int key;
    private final int updateLC;
    private HRC past;

    public UpdateMessage(long originalDC, int key, int updateLC, HRC past) {
        this.originalDC = originalDC;
        this.key = key;
        this.updateLC = updateLC;
        this.past = new HRC(past);
    }

    public long getOriginalDC() {
        return this.originalDC;
    }

    public int getKey() {
        return this.key;
    }

    public int getUpdateLC() {
        return updateLC;
    }

    public HRC getPast() {
        return past;
    }
}
