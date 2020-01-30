package example.saturn;

import example.common.BasicClientInterface;
import example.common.datatypes.Operation;
import example.saturn.StateTreeProtocolInstance;
import example.saturn.StateTreeProtocol;
import example.saturn.datatypes.ReadOperation;
import example.saturn.datatypes.UpdateOperation;
import javafx.util.Pair;
import peersim.core.CommonState;
import peersim.util.ExtendedRandom;

import java.util.*;

import static example.common.Settings.*;

public class Client implements BasicClientInterface {

    private int counter;

    private static final int[] READ_LEVEL_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_PERCENTAGE;

    static {
        READ_LEVEL_PERCENTAGE = Arrays.stream(CLIENT_READ_LEVEL_PERCENTAGE
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        UPDATE_LEVEL_PERCENTAGE = Arrays.stream(CLIENT_UPDATE_LEVEL_PERCENTAGE
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    // Type of Client
    public final int id;
    private final boolean isEager;
    private final int locality;
    private StateTreeProtocolInstance originalDC; // Used for debugging

    // "Static" content
    protected static final ExtendedRandom randomGenerator = CommonState.r;


    // State
    protected boolean justMigrated = false;
    public boolean justMigratedFlag = false;

    protected boolean isWaitingForResult = false;
    protected long nextRestTime = 0;
    protected Operation lastOperation;
    private final List<Operation> operations = new ArrayList<>();


    // Statistics
    public int numberReads = 0;
    public int numberUpdates = 0;
    public long readsTotalLatency = 0;
    public long updatesTotalLatency = 0;
    public long lastOperationTimestamp = 0;
    public int numberMigrations = 0;
    protected long waitingSince;
    public long lastResultReceivedTimestamp = 0;


    private long lastMigrationStart = 0;
    private long totalMigrationTime = 0;


    private HashMap<Integer, Integer> readValues = new HashMap<>();
    public Integer numberReadsTx = 0;
    public Integer number2Reads = 0;
    public long totalReadTxLatency = 0;
    public Integer numberUpdatesTx = 0;
    public long totalUpdateTxLatency = 0;
    public long lastTxTimestamp = 0;
    public long lastTxEndTimestamp = 0;


    public HashMap<Long, Long> context = new HashMap<>();


    public Client(int id, StateTreeProtocolInstance datacenter) {
        this.id = id;
        this.isEager = true;
        this.originalDC = datacenter;
        this.locality = 0;
    }

    @Override
    public final int getId() {
        return id;
    }

    @Override
    public final int getNumberReads() {
        return numberReads;
    }

    @Override
    public final int getNumberUpdates() {
        return numberUpdates;
    }

    @Override
    public final int getNumberMigrations() {
        return numberMigrations;
    }

    @Override
    public final int getLocality() {
        return locality;
    }

    @Override
    public final long getWaitingSince() {
        return waitingSince;
    }

    @Override
    public final float getAverageReadLatency() {
        return (float) readsTotalLatency / numberReads;
    }

    @Override
    public final float getAverageUpdateLatency() {
        return (float) updatesTotalLatency / numberUpdates;
    }

    @Override
    public final float getAverageMigrationTime() {
        return (float) totalMigrationTime;
    }

    @Override
    public int getInstantMigrationsAccept() {
        return 0;
    }

    @Override
    public final boolean isWaiting() {
        return isWaitingForResult;
    }

    public Operation nextOperation() {
        if (lastOperation != null) {
            return null;
        }
        int key = getRandomKey();
        int readOrUpdate = randomGenerator.nextInt(101);
        if (isBetween(readOrUpdate, 0, CLIENT_READ_PERCENTAGE)) {
            this.numberReads++;
            this.lastOperationTimestamp = CommonState.getTime();
            this.lastOperation = new ReadOperation(key);
            return this.lastOperation;
        } else {
            this.numberUpdates++;
            this.lastOperationTimestamp = CommonState.getTime();
            this.lastOperation = new UpdateOperation(key, 0); //o cliente nao esta a 0 sempre

            GlobalContext.newClientLogEntry(getId(), "  update to node " + this.originalDC.getNodeId() + " update number " + this.numberUpdates); //LOG

            return this.lastOperation;

        }
    }

    private int getRandomKey() {
        return originalDC.chooseRandomDataObject();
    }


    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    // My inner OOP is too weak for this abstraction :^(
    public final void receiveReadResult(int key, Object readResult) {
        this.readsTotalLatency += (CommonState.getTime() - this.lastOperationTimestamp);
        this.lastOperation = null;
    }


    public final void receiveUpdateResult(int key, Object updateResult) {
        this.updatesTotalLatency += (CommonState.getTime() - this.lastOperationTimestamp);
        this.lastOperation = null;

    }


    public void migrationStart() {
        lastMigrationStart = CommonState.getTime();
        justMigratedFlag = true;
    }

    @Override
    public void migrationOver(long dcId) {

    }

    @Override
    public void instantMigrationAccept() {

    }

    public void migrationOver() {
        justMigrated = true;
        //isWaitingForResult = false;
        justMigratedFlag = false;
        numberMigrations++;
        totalMigrationTime += (CommonState.getTime() - lastMigrationStart);
    }


    static Map<Integer, Integer> levelsToCount = new HashMap<>();

    private void debugPercentages(int level) {
        levelsToCount.putIfAbsent(level, 0);
        Integer currentVal = levelsToCount.get(level);
        currentVal++;
        levelsToCount.put(level, currentVal);
    }


    protected static boolean isBetween(int x, int lower, int upper) {
        return lower < x && x <= upper;
    }


    public int timestamp() {
        return counter++;
    }

    public boolean doExtraBehaviour() {
        return false;
    }


    public void handleReadResult(int key, Object readResult) {
        return; //TODO

    }

    public void handleUpdateResult(int key, Object updateResult) {
        return; //TODO

    }

    public void get(int key, Object updateResult) {
        // Nothing to do
    }

    public void setDatacenter(StateTreeProtocolInstance dc) {
        this.originalDC = dc;
    }

    public StateTreeProtocol getDatacenter() {
        return this.originalDC;
    }


    public Integer getNumberReadsTx() {
        return numberReadsTx;
    }

    public Integer getNumber2Reads() {
        return number2Reads;
    }

    public float getAverageReadTxLatency() {
        return (float) totalReadTxLatency / numberReadsTx;
    }

    public Integer getNumberUpdatesTx() {
        return numberUpdatesTx;
    }

    public float getAverageUpdateTxLatency() {
        return (float) totalUpdateTxLatency / numberUpdatesTx;
    }

    public Map<Long, Long> getCopyOfContext() {
        return new HashMap<>(this.context);
    }

    public float getAverageTxLatency() {
        return ((float) totalReadTxLatency + totalUpdateTxLatency) / (numberReadsTx + numberUpdatesTx);
    }
}
