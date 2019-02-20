package example.occult;

import example.common.datatypes.DataObject;
import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.Operation;
import example.occult.datatypes.ReadOperation;
import example.occult.datatypes.UpdateOperation;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.util.ExtendedRandom;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class OccultClient implements ClientInterface {

    private static final String PAR_READ_PERCENTAGE = "client_read_percentage";
    private static final String PAR_READ_LEVEL_PERCENTAGES = "client_read_levels_percentage";
    private static final String PAR_UPDATE_PERCENTAGE = "client_update_percentage";
    private static final String PAR_UPDATE_LEVEL_PERCENTAGES = "client_update_levels_percentages";
    private static final String PAR_REST_TIME = "rest_time";
    private static final String PAR_REST_TIME_INTERVAL = "rest_time_interval";
    private static final String PAR_CLIENT_NUMBER_RETRIES = "client_read_retries";
    private static final String PAR_CLIENT_RETRY_INTERVAL = "client_read_retry_interval";

    private static final int READ_PERCENTAGE;
    private static final int UPDATE_PERCENTAGE;
    private static final int[] READ_LEVEL_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_PERCENTAGE;
    private static final long REST_TIME;
    private static final long REST_TIME_INTERVAL;
    private static final int NUMBER_RETRIES;
    protected static final int RETRY_INTERVAL;

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
        NUMBER_RETRIES = Configuration.getInt(PAR_CLIENT_NUMBER_RETRIES);
        RETRY_INTERVAL = Configuration.getInt(PAR_CLIENT_RETRY_INTERVAL);
    }

    private ExtendedRandom randomGenerator = CommonState.r;
    protected final Set<DataObject> possibleDataObjects;
    protected final Map<Integer, Set<DataObject>> dataObjectsPerLevel;

    protected final int id;
    protected final boolean isEager;

    private long waitingSince;
    private int lastReadKey;
    private Operation lastOperation;
    private boolean justMigrated = false;
    private int totalNumberMasterMigration = 0;
    private int numberMigrations = 0;

    protected boolean isWaitingForResult = false;
    protected boolean hadStaleRead = false;
    protected long lastOperationTimestamp = 0;
    protected long lastResultReceivedTimestamp = 0;
    protected long nextRestTime = 0;

    protected int numberReads = 0;
    protected int numberUpdates = 0;
    protected long readsTotalLatency = 0;
    protected long updatesTotalLatency = 0;
    protected int totalStaleReads = 0;
    protected int totalCatchAllReads = 0;
    protected int numberStaleReads = 0;

    /**
     * Maps each shardId to shardstamp
     */
    protected Map<Integer, Integer> clientTimestamp = new HashMap<>();
    protected int catchAllShardStamp = 0;

    protected final StateTreeProtocol datacenter; // Used for debugging
    protected final int locality;





    public OccultClient(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        this.id = id;
        this.dataObjectsPerLevel = new HashMap<>(dataObjectsPerLevel);
        this.possibleDataObjects = dataObjectsPerLevel
                .values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        this.isEager = isEager;
        this.datacenter = datacenter;
        this.locality = locality;

        for (DataObject dataObject : possibleDataObjects) {
            int shardId = GroupsManager.getInstance().getShardId(dataObject.getKey());
            clientTimestamp.put(shardId, 0);
        }
    }

    @Override
    public int getId() {
        return id;
    }


    @Override
    public boolean isWaiting() {
        return isWaitingForResult;
    }

    @Override
    public Operation nextOperation() {
        // Just for debug
        //   System.out.println("I ("+id+") have " + numberStaleReads + " stale reads");
        if (numberStaleReads > NUMBER_RETRIES) {
            throw new RuntimeException("Impossible.");
        }

        if (isWaitingForResult) {
            //    System.out.println("WAITING!");
            return null;
        }


        if (!restTimeOver()) {
            return null;
        }

        // If the client migrated, repeat the last action
        if (justMigrated) {
            //  System.out.println("My (" + id +") migration is over. Im doing a " + lastOperation.getType());
            justMigrated = false;
            if (lastOperation instanceof ReadOperation) {
                ReadOperation op = (ReadOperation) lastOperation;
                op.setMigration(false);
            }
            isWaitingForResult = true;
            waitingSince = CommonState.getTime();
            return lastOperation;
        }

        if (hadStaleRead) {
            //       System.out.println("Had stale read with " + numberStaleReads);
            if (numberStaleReads == NUMBER_RETRIES) {
                int shardId = GroupsManager.getInstance().getShardId(lastReadKey);
                long master = GroupsManager.getInstance().getMasterServer(shardId).getNodeId();
                totalNumberMasterMigration++;
                //         System.out.println("Client migrating to master (" + master + ") for read.");
                lastOperation = new ReadOperation(lastReadKey, true);
            } else {
                lastOperation = new ReadOperation(lastReadKey, false);
            }
            isWaitingForResult = true;
            waitingSince = CommonState.getTime();
            return lastOperation;
        }

        isWaitingForResult = true;
        waitingSince = CommonState.getTime();
        // System.out.println("YAY!");

        int readOrUpdate = randomGenerator.nextInt(101);

        if (isBetween(readOrUpdate, 0, READ_PERCENTAGE )) {
            lastOperation = doRead();
        } else {
            lastOperation = doUpdate();
        }

        if (lastOperation instanceof ReadOperation) {
            //    ReadOperation p = (ReadOperation) lastOperation;
            //  System.out.println("Operation is read and is migrate? " + p.migrateToMaster());
        } else {
            //    System.out.println("Operation is " + lastOperation.getType());
        }

        return lastOperation;
    }

    private Operation doRead() {
        lastOperationTimestamp = CommonState.getTime();

        int randomLevel = getRandomLevel(READ_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        lastReadKey = randomDataObject.getKey();
        return new ReadOperation(randomDataObject.getKey(), false);
    }

    private Operation doUpdate() {
        lastOperationTimestamp = CommonState.getTime();

        int randomLevel = getRandomLevel(UPDATE_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        return new UpdateOperation(randomDataObject.getKey(), clientTSCopy(), getNumberCatchAll());
    }

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

    @Override
    public void receiveReadResult(long server, OccultReadResult readResult) {
        boolean readFromSlave = !readResult.isMaster();
        int shardId = readResult.getShardId();
        int clientShardStamp = getShardStampFromCS(shardId);
        int shardStamp = readResult.getShardStamp();

        if (readFromSlave && shardStamp < clientShardStamp) {
            lastResultReceivedTimestamp = CommonState.getTime();
            nextRestTime = RETRY_INTERVAL;
            numberStaleReads++;
            totalStaleReads++;
            hadStaleRead = true;
        } else {
            numberStaleReads = 0;
            hadStaleRead = false;
            lastResultReceivedTimestamp = CommonState.getTime();
            numberReads++;
            readsTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);

            updateClientTimestampFromRead(clientTimestamp, catchAllShardStamp,
                    readResult.getDeps(), readResult.getCatchAll());
        }
        isWaitingForResult = false;
    }

    public abstract int getShardStampFromCS(int shardId);

    public abstract void updateClientTimestampFromRead(
            Map<Integer, Integer> clientTimestamp,
            int clientCatchAll,
            Map<Integer, Integer> deps,
            int depsCatchAll);

    @Override
    public void migrationOver() {
        justMigrated = true;
        isWaitingForResult = false;
        numberMigrations++;
    }

    @Override
    public int getNumberReads() {
        return numberReads;
    }

    @Override
    public int getNumberUpdates() {
        return numberUpdates;
    }

    @Override
    public int getNumberMigrations() {
        return numberMigrations;
    }

    @Override
    public int getNumberStaleReads() {
        return totalStaleReads;
    }

    @Override
    public int getNumberMasterMigrations() {
        return totalNumberMasterMigration;
    }

    @Override
    public int getNumberCatchAll() {
        return totalCatchAllReads;
    }

    @Override
    public int getLocality() {
        return locality;
    }

    @Override
    public long getWaitingSince() {
        return waitingSince;
    }

    @Override
    public float getAverageReadLatency() {
        return (float) readsTotalLatency / numberReads;
    }

    @Override
    public float getAverageUpdateLatency() {
        return (float) updatesTotalLatency / numberUpdates;
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

    private DataObject chooseRandomDataObject(int level) {
        int bound = dataObjectsPerLevel.get(level).size();
        if (bound == 0) {
            bound = 1;
        }
        return dataObjectsPerLevel.get(level).stream()
                .skip(randomGenerator.nextInt(bound))
                .findFirst().get();
    }

    private Map<Integer, Integer> clientTSCopy() {
        return new HashMap<>(clientTimestamp);
    }

    private boolean isBetween(int x, int lower, int upper) {
        return lower <= x && x <= upper;
    }

}
