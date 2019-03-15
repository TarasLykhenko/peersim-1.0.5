package example.capstonematrix.datatypes;

public class ReadResult {

    private HRC hierarchicalRegionalClock;

    public ReadResult(HRC hierarchicalRegionalClock) {
        this.hierarchicalRegionalClock = new HRC(hierarchicalRegionalClock);
    }

    public HRC getHRC() {
        return hierarchicalRegionalClock;
    }
}
