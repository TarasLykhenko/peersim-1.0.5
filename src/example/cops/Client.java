package example.cops;

import example.cops.datatypes.DataObject;
import example.cops.datatypes.Operation;
import example.cops.datatypes.ReadOperation;
import example.cops.datatypes.UpdateOperation;
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

public class Client {

    private static final String PAR_READ_PERCENTAGE = "client_read_percentage";
    private static final String PAR_READ_LEVEL_PERCENTAGES = "client_read_levels_percentage";
    private static final String PAR_UPDATE_PERCENTAGE = "client_update_percentage";
    private static final String PAR_UPDATE_LEVEL_PERCENTAGES = "client_update_levels_percentages";

    private static final int READ_PERCENTAGE;
    private static final int UPDATE_PERCENTAGE;
    private static final int[] READ_LEVEL_PERCENTAGE;
    private static final int[] UPDATE_LEVEL_PERCENTAGE;

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
    }

    private ExtendedRandom randomGenerator = CommonState.r;
    private final Set<DataObject> possibleDataObjects;
    private final Map<Integer, Set<DataObject>> dataObjectsPerLevel;
    private final List<Operation.Type> operations = new ArrayList<>();

    private final int id;
    private final boolean isEager;
    private final int locality;

    /**
     * COPS Client state : A map of keys to version.
     * Every time a client reads, he adds the key and version on the map.
     * Every time a client writes, he uses the current context on the write, clears it
     * and adds the key and version of the write to the context
     */
    private Map<Integer, Integer> context = new HashMap<>();

    public Map<Integer, Integer> getCopyOfContext() {
        return new HashMap<>(context);
    }

    private final StateTreeProtocol datacenter; // Used for debugging


    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        this.id = id;
        this.dataObjectsPerLevel = new HashMap<>(dataObjectsPerLevel);
        this.possibleDataObjects = dataObjectsPerLevel
                .values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        this.isEager = isEager;
        this.datacenter = datacenter;
        this.locality = locality;
    }

    public int getId() {
        return id;
    }


    private boolean isWaitingForResult = false;
    Operation nextOperation() {
        if (isWaitingForResult) {
            System.out.println("WAITING!");
            return null;
        }

        int readOrUpdate = randomGenerator.nextInt(101);

        if (isBetween(readOrUpdate, 0, READ_PERCENTAGE )) {
            return doRead();
        } else {
            return doUpdate();
        }
    }

    public void receiveReadResult(Integer key, Integer version) {
        context.put(key, version);
        isWaitingForResult = false;
    }

    public void receiveUpdateResult(Integer key, Integer version) {
        // Can't clear the context because of partial replication.
        // context.clear();
        context.put(key, version);
        isWaitingForResult = false;
    }

    private Operation doRead() {
        isWaitingForResult = true;
        int randomLevel = getRandomLevel(READ_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        operations.add(Operation.Type.READ);
        return new ReadOperation(randomDataObject.getKey());
    }

    private Operation doUpdate() {
        isWaitingForResult = true;
        int randomLevel = getRandomLevel(UPDATE_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        operations.add(Operation.Type.UPDATE);
        return new UpdateOperation(randomDataObject.getKey(), context);
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
        return dataObjectsPerLevel.get(level).stream()
                .skip(randomGenerator.nextInt(dataObjectsPerLevel.get(level).size()))
                .findFirst().get();
    }

    private static boolean isBetween(int x, int lower, int upper) {
        return lower <= x && x <= upper;
    }

    public Client clone() {
        return new Client(id, isEager, new HashMap<>(dataObjectsPerLevel), datacenter, locality);
    }
}
