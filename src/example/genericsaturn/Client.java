package example.genericsaturn;

import example.genericsaturn.datatypes.DataObject;
import example.genericsaturn.datatypes.Operation;
import example.genericsaturn.datatypes.ReadOperation;
import example.genericsaturn.datatypes.RemoteReadOperation;
import example.genericsaturn.datatypes.UpdateOperation;
import peersim.config.Configuration;
import sun.security.krb5.Config;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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

    private final Set<DataObject> possibleDataObjects;
    private final Map<Integer, Set<DataObject>> dataObjectsPerLevel;

    private Random randomGenerator = new Random();
    private final int id;
    private final boolean isEager;
    private int counter;
    private final int locality;

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

    public int timestamp() {
        return counter++;
    }


    Operation nextOperation() {
        int readOrUpdate = randomGenerator.nextInt(101);

        if (isBetween(readOrUpdate, 0, READ_PERCENTAGE )) {
            return doRead();
        } else {
            return doUpdate();
        }
    }

    private Operation doRead() {
        int randomLevel = getRandomLevel(READ_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        return new ReadOperation(randomDataObject.getTotalCounter());
    }

    //TODO este do update deve estar errado
    private Operation doUpdate() {
        int randomLevel = getRandomLevel(UPDATE_LEVEL_PERCENTAGE);
        DataObject randomDataObject = chooseRandomDataObject(randomLevel);
        return new UpdateOperation(randomDataObject.getTotalCounter(),
                1,
                1,
                randomDataObject.getUniqueId());
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
