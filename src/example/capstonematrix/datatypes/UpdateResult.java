package example.capstonematrix.datatypes;

public class UpdateResult {

    private long cloudletId;
    private HRC hrc;

    public UpdateResult(long cloudletId, HRC hrc) {
        this.cloudletId = cloudletId;
        this.hrc = new HRC(hrc);
    }

    public long getCloudletId() {
        return this.cloudletId;
    }

    public HRC getHRC() {
        return this.hrc;
    }
}
