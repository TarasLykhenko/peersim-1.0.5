package example.common;

import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;
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

import static example.common.Settings.CLIENT_MIGRATION_ODDS;
import static example.common.Settings.CLIENT_OBJECT_READ_LVL_0;
import static example.common.Settings.CLIENT_OBJECT_READ_LVL_1;
import static example.common.Settings.CLIENT_OBJECT_READ_LVL_2;
import static example.common.Settings.CLIENT_OBJECT_READ_LVL_3;
import static example.common.Settings.CLIENT_OBJECT_UPDATE_LVL_0;
import static example.common.Settings.CLIENT_OBJECT_UPDATE_LVL_1;
import static example.common.Settings.CLIENT_OBJECT_UPDATE_LVL_2;
import static example.common.Settings.CLIENT_OBJECT_UPDATE_LVL_3;
import static example.common.Settings.CLIENT_READ_PERCENTAGE;
import static example.common.Settings.REST_TIME;
import static example.common.Settings.REST_TIME_INTERVAL;

public abstract class AbstractBaseClient implements BasicClientInterface {

    private static final int[] MIGRATION_LEVELS_PERCENTAGE;

    private static final int[] READ_LEVEL_0_PERCENTAGE;
    private static final int[] READ_LEVEL_1_PERCENTAGE;
    private static final int[] READ_LEVEL_2_PERCENTAGE;
    private static final int[] READ_LEVEL_3_PERCENTAGE;

    private static final int[] UPDATE_LEVEL_0_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_1_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_2_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_3_PERCENTAGE;

    static {
        MIGRATION_LEVELS_PERCENTAGE = convertStringArray(CLIENT_MIGRATION_ODDS);

        READ_LEVEL_0_PERCENTAGE = convertStringArray(CLIENT_OBJECT_READ_LVL_0);
        READ_LEVEL_1_PERCENTAGE = convertStringArray(CLIENT_OBJECT_READ_LVL_1);
        READ_LEVEL_2_PERCENTAGE = convertStringArray(CLIENT_OBJECT_READ_LVL_2);
        READ_LEVEL_3_PERCENTAGE = convertStringArray(CLIENT_OBJECT_READ_LVL_3);

        UPDATE_LEVEL_0_PERCENTAGE = convertStringArray(CLIENT_OBJECT_UPDATE_LVL_0);
        UPDATE_LEVEL_1_PERCENTAGE = convertStringArray(CLIENT_OBJECT_UPDATE_LVL_1);
        UPDATE_LEVEL_2_PERCENTAGE = convertStringArray(CLIENT_OBJECT_UPDATE_LVL_2);
        UPDATE_LEVEL_3_PERCENTAGE = convertStringArray(CLIENT_OBJECT_UPDATE_LVL_3);
    }


    private static int[] convertStringArray(String stringArray) {
        return Arrays.stream(stringArray
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
    }

    // Type of Client
    private final int id;
    private final boolean isEager;
    private final int locality;
    private long currentDCId;
    private final BasicStateTreeProtocol originalDC; // Used for debugging

    // "Static" content
    private static final ExtendedRandom randomGenerator = CommonState.r;
    private final Set<DataObject> possibleDataObjects;
    private final Map<Integer, DataObject> keysToDataObjects;
    private final Map<Integer, Set<DataObject>> dataObjectsPerLevel;

    private final Map<Long, Map<Integer, Set<DataObject>>> datacentersToObjectsPerLevel;
    private final Map<Integer, Set<Long>> exclusiveDCsPerLevel;


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

    private long lastMigrationStart = 0;
    private long totalMigrationTime = 0;


    public AbstractBaseClient(int id, boolean isEager,
                              Map<Integer, Set<DataObject>> dataObjectsPerLevel,
                              BasicStateTreeProtocol datacenter, int locality,
                              GroupsManagerInterface groupsManager) {
        this.id = id;
        this.dataObjectsPerLevel = new HashMap<>(dataObjectsPerLevel);
        this.possibleDataObjects = dataObjectsPerLevel
                .values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        this.keysToDataObjects = possibleDataObjects
                .stream()
                .collect(Collectors.toMap(DataObject::getKey, e -> e));
        this.isEager = isEager;
        this.originalDC = datacenter;
        this.currentDCId = datacenter.getNodeId();
        this.locality = locality;

        datacentersToObjectsPerLevel = groupsManager.getDataCenterIdsDataObjects();
        exclusiveDCsPerLevel = groupsManager.getExclusiveNodeToLevelNeighbourIds(originalDC.getNodeId());
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
        return (float) totalMigrationTime / numberMigrations;
    }

    @Override
    public final boolean isWaiting() {
        return isWaitingForResult;
    }

    public Operation nextOperation() {

        if (isWaitingForResult) {
            System.out.println("Waitign for result");
            return null;
        }

        if (!restTimeOver()) {
            System.out.println("Resting");
            return null;
        }

        // If the client migrated, repeat the last action
        if (justMigrated) {
            justMigrated = false;
            isWaitingForResult = true;
            waitingSince = CommonState.getTime();
            System.out.println("Migration over");
            return lastOperation;
        }

        boolean extraBehaviourResult = doExtraBehaviour();
        if (extraBehaviourResult) {
            System.out.println("had extra behaviour");
            return lastOperation;
        }

        // First choose where to migrate to.
        int level = getRandomLevel(MIGRATION_LEVELS_PERCENTAGE);
        long nextMigrationDC = getMigrationDC(level);

        isWaitingForResult = true;
        waitingSince = CommonState.getTime();

        int readOrUpdate = randomGenerator.nextInt(101);

        if (isBetween(readOrUpdate, 0, CLIENT_READ_PERCENTAGE)) {
            lastOperation = doRead(level, nextMigrationDC);
        } else {
            lastOperation = doUpdate(level, nextMigrationDC);
        }

        if (Settings.PRINT_INFO) {
            System.out.println("(" + lastOperation.getType() + ") (mlvl: " + level + " | Client " + id + " | originDC : " + originalDC.getNodeId() + " | currentDC: " + currentDCId
                    + " | nextDC: " + nextMigrationDC + " | doing op on " + keysToDataObjects.get(lastOperation.getKey()).getDebugInfo());
        }

        lastOperation.setDatacenter(nextMigrationDC);

        return lastOperation;
    }

    /**
     * Returns the DC to migrate to (can be the own server)
     */
    private long getMigrationDC(int level) {
        Set<Long> dcIds = exclusiveDCsPerLevel.get(level);
        int bound = dcIds.size();
        if (bound == 0) {
            bound = 1;
        }
        Long nextDC = dcIds.stream().skip(randomGenerator.nextInt(bound))
                .findFirst().get();

        return nextDC;

    }

    /**
     * Returns true if it should return the lastOperation.
     *
     * @return
     */
    public abstract boolean doExtraBehaviour();

    private Operation doRead(int level, long dataCenter) {
        lastOperationTimestamp = CommonState.getTime();
        numberReads++;

        int[] readLevelsPercentage = getReadLevelsPercentage(level);
        int readLevel = getRandomLevel(readLevelsPercentage);
        DataObject dataObject = chooseRandomDataObject(readLevel, dataCenter);

        return specificDoRead(dataObject);
    }

    private int[] getReadLevelsPercentage(int level) {
        if (level == 0) {
            return READ_LEVEL_0_PERCENTAGE;
        } else if (level == 1) {
            return READ_LEVEL_1_PERCENTAGE;
        } else if (level == 2) {
            return READ_LEVEL_2_PERCENTAGE;
        } else if (level == 3) {
            return READ_LEVEL_3_PERCENTAGE;
        } else {
            throw new NullPointerException("Incorrect level");
        }
    }

    private Operation doUpdate(int level, long dataCenter) {
        lastOperationTimestamp = CommonState.getTime();
        numberUpdates++;

        int[] updateLevelsPercentage = getUpdateLevelsPercentage(level);
        int updateLevel = getRandomLevel(updateLevelsPercentage);
        DataObject dataObject = chooseRandomDataObject(updateLevel, dataCenter);

        return specificDoUpdate(dataObject);
    }

    private int[] getUpdateLevelsPercentage(int level) {
        if (level == 0) {
            return UPDATE_LEVEL_0_PERCENTAGE;
        } else if (level == 1) {
            return UPDATE_LEVEL_1_PERCENTAGE;
        } else if (level == 2) {
            return UPDATE_LEVEL_2_PERCENTAGE;
        } else if (level == 3) {
            return UPDATE_LEVEL_3_PERCENTAGE;
        } else {
            throw new NullPointerException("Incorrect level");
        }
    }

    public abstract Operation specificDoRead(DataObject dataObject);

    public abstract Operation specificDoUpdate(DataObject dataObject);

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
    public void migrationStart() {
        lastMigrationStart = CommonState.getTime();
    }

    @Override
    public void migrationOver(long dcId) {
        currentDCId = dcId;
        justMigrated = true;
        isWaitingForResult = false;
        numberMigrations++;
        totalMigrationTime += (CommonState.getTime() - lastMigrationStart);
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
        throw new RuntimeException("The settings file must have a wrong percentage vector.");
    }

    static Map<Integer, Integer> levelsToCount = new HashMap<>();

    private void debugPercentages(int level) {
        levelsToCount.putIfAbsent(level, 0);
        Integer currentVal = levelsToCount.get(level);
        currentVal++;
        levelsToCount.put(level, currentVal);
    }

    protected DataObject chooseRandomDataObject(int level, long datacenter) {
        int bound = datacentersToObjectsPerLevel.get(datacenter).get(level).size();
        if (bound == 0) {
            bound = 1;
        }

        return datacentersToObjectsPerLevel.get(datacenter).get(level).stream()
                .skip(randomGenerator.nextInt(bound))
                .findFirst().get();
    }

    private static boolean isBetween(int x, int lower, int upper) {
        return lower < x && x <= upper;
    }
}
