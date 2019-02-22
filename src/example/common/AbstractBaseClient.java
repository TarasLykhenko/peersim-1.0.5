package example.common;

import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.util.ExtendedRandom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractBaseClient implements BasicClientInterface {

    private static final String PAR_READ_PERCENTAGE = "client_read_percentage";
    private static final String PAR_READ_LEVEL_PERCENTAGES = "client_read_levels_percentage";
    private static final String PAR_UPDATE_PERCENTAGE = "client_update_percentage";
    private static final String PAR_UPDATE_LEVEL_PERCENTAGES = "client_update_levels_percentages";
    private static final String PAR_REST_TIME = "rest_time";
    private static final String PAR_REST_TIME_INTERVAL = "rest_time_interval";

    private static final int READ_PERCENTAGE;
    private static final int UPDATE_PERCENTAGE;
    private static final int[] READ_LEVEL_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_PERCENTAGE;
    private static final long REST_TIME;
    private static final long REST_TIME_INTERVAL;

    static {
        READ_PERCENTAGE = Configuration.getInt(PAR_READ_PERCENTAGE);
        UPDATE_PERCENTAGE = Configuration.getInt(PAR_UPDATE_PERCENTAGE);
        String rawReadLevelPercentage = Configuration.getString(PAR_READ_LEVEL_PERCENTAGES);
        String rawUpdateLevelPercentage = Configuration.getString(PAR_UPDATE_LEVEL_PERCENTAGES);
        READ_LEVEL_PERCENTAGE = Arrays.stream(rawReadLevelPercentage
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        UPDATE_LEVEL_PERCENTAGE = Arrays.stream(rawUpdateLevelPercentage
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        REST_TIME = Configuration.getLong(PAR_REST_TIME);
        REST_TIME_INTERVAL = Configuration.getLong(PAR_REST_TIME_INTERVAL);
    }

    // Type of Client
    private final int id;
    private final boolean isEager;
    private final int locality;
    private final BasicStateTreeProtocol datacenter; // Used for debugging

    // "Static" content
    private static final ExtendedRandom randomGenerator = CommonState.r;
    private final Set<DataObject> possibleDataObjects;
    private final Map<Integer, Set<DataObject>> dataObjectsPerLevel;


    // State
    private boolean justMigrated = false;
    protected boolean isWaitingForResult = false;
    protected long nextRestTime = 0;
    protected Operation lastOperation;
    private final List<Operation> operations = new ArrayList<>();


    // Statistics
    protected int numberReads = 0;
    protected int numberUpdates = 0;
    protected long readsTotalLatency = 0;
    protected long updatesTotalLatency = 0;
    protected long lastOperationTimestamp = 0;
    private int numberMigrations = 0;
    protected long waitingSince;
    protected long lastResultReceivedTimestamp = 0;



    public AbstractBaseClient(int id, boolean isEager,
                              Map<Integer, Set<DataObject>> dataObjectsPerLevel,
                              BasicStateTreeProtocol datacenter, int locality) {
        this.id = id;
        this.dataObjectsPerLevel = new HashMap<>(dataObjectsPerLevel);
        this.possibleDataObjects = dataObjectsPerLevel
                .values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        this.isEager = isEager;
        this.datacenter = datacenter;
        this.locality = locality;
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
    public final boolean isWaiting() {
        return isWaitingForResult;
    }

    public Operation nextOperation() {
        if (isWaitingForResult) {
            return null;
        }

        if (!restTimeOver()) {
            return null;
        }

        // If the client migrated, repeat the last action
        if (justMigrated) {
            justMigrated = false;
            isWaitingForResult = true;
            waitingSince = CommonState.getTime();
            return lastOperation;
        }

        boolean extraBehaviourResult = doExtraBehaviour();
        if (extraBehaviourResult) {
            return lastOperation;
        }

        isWaitingForResult = true;
        waitingSince = CommonState.getTime();

        int readOrUpdate = randomGenerator.nextInt(101);

        if (isBetween(readOrUpdate, 0, READ_PERCENTAGE )) {
            lastOperation = doRead();
        } else {
            lastOperation = doUpdate();
        }

        return lastOperation;
    }

    /**
     * Returns true if it should return the lastOperation.
     * @return
     */
    public abstract boolean doExtraBehaviour();

    private Operation doRead() {
        lastOperationTimestamp = CommonState.getTime();
        numberReads++;

        int readLevel = getRandomLevel(READ_LEVEL_PERCENTAGE);

        return specificDoRead(readLevel);
    }

    private Operation doUpdate() {
        lastOperationTimestamp = CommonState.getTime();
        numberUpdates++;

        int updateLevel = getRandomLevel(UPDATE_LEVEL_PERCENTAGE);

        return specificDoUpdate(updateLevel);
    }

    public abstract Operation specificDoRead(int readLevel);

    public abstract Operation specificDoUpdate(int updateLevel);

    /*
    private Operation doRead() {
        lastOperationTimestamp = CommonState.getTime();
        lastOperationType = Type.READ;
        numberReads++;

        int randomLevel = getRandomLevel(READ_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        operations.add(Type.READ);
        return new ReadOperation(randomDataObject.getKey());
    }

    private Operation doUpdate() {
        lastOperationTimestamp = CommonState.getTime();
        lastOperationType = Type.UPDATE;
        numberUpdates++;

        int randomLevel = getRandomLevel(UPDATE_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        operations.add(Type.UPDATE);
        return new UpdateOperation(randomDataObject.getKey(), context);
    }
    */

    private boolean restTimeOver() {
        if (isEager) {
            return true;
        }

        if (lastResultReceivedTimestamp + nextRestTime < CommonState.getTime()) {
            nextRestTime =
                    CommonState.r.nextLong(2 * REST_TIME_INTERVAL) -
                            REST_TIME_INTERVAL + REST_TIME;
            return true;
        } else {
            return false;
        }
    }

    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    // My inner OOP is too weak for this abstraction :^(
    public final void receiveReadResult(int key, Object readResult) {
        handleReadResult(key, readResult);

        isWaitingForResult = false;
        lastResultReceivedTimestamp = CommonState.getTime();
        readsTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);
    }

    public abstract void handleReadResult(int key, Object readResult);

    public final void receiveUpdateResult(int key, Object updateResult) {
        handleUpdateResult(key, updateResult);

        isWaitingForResult = false;
        lastResultReceivedTimestamp = CommonState.getTime();
        updatesTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);
    }

    public abstract void handleUpdateResult(int key, Object updateResult);

    @Override
    public void migrationOver() {
        justMigrated = true;
        isWaitingForResult = false;
        numberMigrations++;
    }

    private int getRandomLevel(int[] levelPercentages) {
        int chosenValue = randomGenerator.nextInt(10000);
        int currentSum = 0;
        for (int i = 0; i < levelPercentages.length; i++) {
            currentSum += levelPercentages[i] * 100;
            if (chosenValue < currentSum) {
                debugPercentages(i);
                return i;
            }
        }
        throw new RuntimeException("Well..the odds didn't work correctly.");
    }

    static Map<Integer, Integer> levelsToCount = new HashMap<>();
    private void debugPercentages(int level) {
        levelsToCount.putIfAbsent(level, 0);
        Integer currentVal = levelsToCount.get(level);
        currentVal++;
        levelsToCount.put(level, currentVal);
    }

    protected DataObject chooseRandomDataObject(int level) {
        int bound = dataObjectsPerLevel.get(level).size();
        if (bound == 0) {
            bound = 1;
        }

        return dataObjectsPerLevel.get(level).stream()
                .skip(randomGenerator.nextInt(bound))
                .findFirst().get();
    }

    private static boolean isBetween(int x, int lower, int upper) {
        return lower < x && x <= upper;
    }
}
