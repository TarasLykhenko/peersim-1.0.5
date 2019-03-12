package example.capstonematrix.datatypes;

public class UpdateResult {

    private long cloudletId;
    private int cloudletCounter;

    public UpdateResult(long cloudletId, int cloudletCounter) {
        this.cloudletId = cloudletId;
        this.cloudletCounter = cloudletCounter;
    }

    public long getCloudletId() {
        return this.cloudletId;
    }

    public int getCloudletCounter() {
        return this.cloudletCounter;
    }
}
