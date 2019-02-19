package example.occult.temporal_compression;

import example.common.datatypes.DataObject;
import example.occult.ClientInterface;
import example.occult.GroupsManager;
import example.occult.StateTreeProtocol;
import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.Operation;
import example.occult.datatypes.Operation.Type;
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

public class Client implements ClientInterface {

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
    private static final int RETRY_INTERVAL;

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
    private final Set<DataObject> possibleDataObjects;
    private final Map<Integer, Set<DataObject>> dataObjectsPerLevel;

    private final int id;
    private final boolean isEager;

    private boolean isWaitingForResult = false;
    long waitingSince;
    private int lastReadKey;
    private boolean hadStaleRead = false;
    private int numberStaleReads = 0;
    private Operation lastOperation;
    private Type lastOperationType;
    private long lastOperationTimestamp = 0;
    private long lastResultReceivedTimestamp = 0;
    private boolean justMigrated = false;
    private long nextRestTime = 0;

    int numberReads = 0;
    int numberUpdates = 0;
    int numberMigrations = 0;
    long readsTotalLatency = 0;
    long updatesTotalLatency = 0;
    int totalFalseShardRead = 0;
    int totalStaleReads = 0;
    int totalNumberMasterMigration = 0;


    private int MAX_CTS_SIZE = 9;

    /**
     * Maps each shardId to shardstamp
     */
    private Map<Integer, Integer> clientTimestamp = new HashMap<>(MAX_CTS_SIZE);
    private int catchAllShardStamp = 0;


    private final StateTreeProtocol datacenter; // Used for debugging
    final int locality;


    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        this.id = id;
        this.dataObjectsPerLevel = new HashMap<>(dataObjectsPerLevel);
        this.possibleDataObjects = dataObjectsPerLevel
                .values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        this.isEager = isEager;
        this.datacenter = datacenter;
        this.locality = locality;
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
                System.out.println("Client migrating to master (" + master + ") for read.");
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

        if (isBetween(readOrUpdate, 0, READ_PERCENTAGE)) {
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
        lastOperationType = Type.READ;

        int randomLevel = getRandomLevel(READ_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        lastReadKey = randomDataObject.getKey();
        return new ReadOperation(randomDataObject.getKey(), false);
    }

    private Operation doUpdate() {
        lastOperationTimestamp = CommonState.getTime();
        lastOperationType = Type.UPDATE;

        int randomLevel = getRandomLevel(UPDATE_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        return new UpdateOperation(randomDataObject.getKey(), clientTSCopy(), catchAllShardStamp);
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
        //   System.out.println("I ("+id+") received a read from " + server);
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
            //    System.out.println("Increasing stale reads, now at " + numberStaleReads + " master:" + !readFromSlave);
        } else {
            //    System.out.println("Read was good! master:" + !readFromSlave);
            numberStaleReads = 0;
            hadStaleRead = false;
            lastResultReceivedTimestamp = CommonState.getTime();
            numberReads++;
            readsTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);

            Map<Integer, Integer> deps = readResult.getDeps();
            int readCatchAll = readResult.getCatchAll();
            mergeClientTimeStamps(clientTimestamp, catchAllShardStamp, deps, readCatchAll);
        }
        isWaitingForResult = false;
    }

    private Integer getShardStampFromCS(int shardId) {
        if (clientTimestamp.size() == MAX_CTS_SIZE) {
            if (clientTimestamp.get(shardId) == null) {
                totalFalseShardRead++;
                return catchAllShardStamp;
            } else {
                return clientTimestamp.get(shardId);
            }
        } else {
            return 0;
        }
    }

    @Override
    public void receiveUpdateResult(Integer shardId, Integer updateShardStamp) {
        updateClientTimeStamp(shardId, updateShardStamp);

        //     System.out.println("Received update!");
        isWaitingForResult = false;
        lastResultReceivedTimestamp = CommonState.getTime();
        numberUpdates++;
        updatesTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);
    }

    void updateClientTimeStamp(Integer shardId, Integer updateShardStamp) {
        // Scenario 1: Client timestamp contains the entry, check if update
        // System.out.println("My ts: " + clientTimestamp);
        // System.out.println("ShardID: " + shardId + " | updateStamp: " + updateShardStamp);
        if (clientTimestamp.containsKey(shardId)) {
            // System.out.println("Updating...");
            int oldShardStamp = clientTimestamp.get(shardId);
            if (updateShardStamp > oldShardStamp) {
                clientTimestamp.put(shardId, updateShardStamp);
            }
        } else if (clientTimestamp.size() < MAX_CTS_SIZE) {
            // System.out.println("Adding..");
            // Scenario 2: Client TS does not contain the entry but
            // has space for more entries, therefore add it
            clientTimestamp.put(shardId, updateShardStamp);
        } else {
            // System.out.println("Adding to catch all.");
            // Scenario 3: ShardID not explicitly tracked, add Catchall
            int lowestShardId = 0;
            int lowestShardIdStamp = Integer.MAX_VALUE;
            for (Integer trackedShardId : clientTimestamp.keySet()) {
                if (clientTimestamp.get(trackedShardId) < lowestShardIdStamp) {
                    lowestShardIdStamp = clientTimestamp.get(trackedShardId);
                    lowestShardId = trackedShardId;
                }
            }

            clientTimestamp.remove(lowestShardId);
            if (lowestShardIdStamp < catchAllShardStamp) {
                catchAllShardStamp = lowestShardIdStamp;
            }
        }
        if (clientTimestamp.size() > MAX_CTS_SIZE) {
            // System.out.println("My ts is " + clientTimestamp);
            throw new RuntimeException("update failed");
        }
    }

    private void mergeClientTimeStamps(
            Map<Integer, Integer> timeStamp1, int catchAllOne,
            Map<Integer, Integer> timestamp2, int catchAllTwo) {
        // We start the merge by copying the first map.
        Map<Integer, Integer> result = new HashMap<>(timeStamp1);
       // System.out.println("M1: " + timeStamp1);
       // System.out.println("M2: " + timestamp2);
       // System.out.println("CatchAll 1:" + catchAllOne);
       // System.out.println("Catch all 2:" + catchAllTwo);
        int highestCatchAll = 0;
        if (catchAllOne < catchAllTwo) {
            highestCatchAll = catchAllTwo;
        } else {
            highestCatchAll = catchAllOne;
        }

        for (Integer shardIdTwo : timestamp2.keySet()) {
            int shardStampTwo = timestamp2.get(shardIdTwo);

            // Case 1: Both maps contain the same shardIdTwo, update
            if (result.containsKey(shardIdTwo) && result.get(shardIdTwo) < shardStampTwo) {
                result.put(shardIdTwo, shardStampTwo);
            } else if (result.size() < MAX_CTS_SIZE) {
                // Case 2: Client Timestamp can have more entries, add to result
                result.put(shardIdTwo, shardStampTwo);
            } else {
                // Case 3: Map 2 contains a key that Map 1 does not, check if
                // its value is higher
                for (Integer resultShardId : result.keySet()) {
                    int resultShardStamp = result.get(resultShardId);
                    if (resultShardStamp < shardStampTwo) {
                        result.remove(resultShardId);
                        result.put(shardIdTwo, shardStampTwo);
                        if (resultShardStamp > highestCatchAll) {
                         //   System.out.println("Updating catchall");
                            highestCatchAll = resultShardStamp;
                        }
                        break;
                    }
                }
            }
        }
        if (result.size() > MAX_CTS_SIZE) {
            // System.out.println("My ts is " + clientTimestamp);
            throw new RuntimeException("Merge failed");
        }

        //System.out.println("Merge: " + result);
        //System.out.println("Catch all result: " + highestCatchAll);
        this.catchAllShardStamp = highestCatchAll;
        this.clientTimestamp = result;
    }

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

    private static boolean isBetween(int x, int lower, int upper) {
        return lower <= x && x <= upper;
    }

    public Client clone() {
        return new Client(id, isEager, new HashMap<>(dataObjectsPerLevel), datacenter, locality);
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
        return numberStaleReads;
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
}
