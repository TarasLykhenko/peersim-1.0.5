package example.occult;

import example.occult.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import sun.security.krb5.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class InitTreeProtocol implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------

    private static final String PAR_TREE_PROT = "tree_protocol";

    private static final String PAR_MAX_CLIENTS = "clients_per_datacenter";
    private static final String PAR_TOTAL_OBJECTS_PER_DATACENTER = "total_objects_per_datacenter";
    private static final String PAR_LEVELS_PERCENTAGE = "levels_percentage";
    private static final String PAR_CLIENTS_EAGER = "clients_eager_percentage";
    private static final String PAR_CLIENTS_LOCALITY_PERCENTAGE = "client_locality_percentage";
    private static final String PAR_NUMBER_SHARDS_PER_GROUND = "shards_per_object_group";


    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final int tree;
    private final int maxClients;
    private final int totalObjects;
    private final int[] levelsPercentage;
    private final int[] clientsLocalityPercentages;
    private final int clientEagerPercentage;
    private final int numberShardsPerGroup;


    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    public InitTreeProtocol(String prefix) {
        tree = Configuration.getPid(prefix + "." + PAR_TREE_PROT);
        maxClients = Configuration.getInt(PAR_MAX_CLIENTS);
        totalObjects = Configuration.getInt(PAR_TOTAL_OBJECTS_PER_DATACENTER);
        String rawLevelPercentage = Configuration.getString(PAR_LEVELS_PERCENTAGE);
        levelsPercentage = Arrays.stream(rawLevelPercentage
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        String rawClientsLocalityPercentage = Configuration.getString(PAR_CLIENTS_LOCALITY_PERCENTAGE);
        clientsLocalityPercentages = Arrays.stream(rawClientsLocalityPercentage
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        clientEagerPercentage = Configuration.getInt(PAR_CLIENTS_EAGER);
        numberShardsPerGroup = Configuration.getInt(PAR_NUMBER_SHARDS_PER_GROUND);
    }

    // ------------------------------------------------------------------------
    // Methods
    // ------------------------------------------------------------------------

    /**
     * This is responsible for generating data objects that are placed in
     * Datacenters in a deterministic way
     */
    public boolean execute() {
        initStateTreeProtocol();
        // Generate first 1 level (local) data objects, then 2 levels (regional)
        // then 3 levels (country) ...
        Set<StateTreeProtocol> datacenters = getDatacenters();

        populateDatacenterLevelsToDatacenters(datacenters);

        generateDataObjects(datacenters);
        generateClients(datacenters);

        debugPrintStatus(datacenters);

        return false;
    }

    private int dataObjectCounter = 0;
    private int currentShardId = 1_000_000_000;
    private void generateDataObjects(Set<StateTreeProtocol> datacenters) {
        for (int level = 0; level < levelsPercentage.length; level++) {
            Set<StateTreeProtocol> seenDatacenters = new HashSet<>();

            for (StateTreeProtocol datacenter : datacenters) {
                if (seenDatacenters.contains(datacenter)) {
                    continue;
                }

                Set<StateTreeProtocol> datacenterGroup = new HashSet<>();
                datacenterGroup.add(datacenter);
                datacenterGroup.addAll(datacenter.getLevelsToNodes(level));
                seenDatacenters.addAll(datacenterGroup);

                generateDataObjectsForGroup(datacenterGroup, level, levelsPercentage[level]);
            }
        }
    }

    private void initStateTreeProtocol() {
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocol treeProtocol = (StateTreeProtocol) node.getProtocol(tree);
            treeProtocol.setNodeId(node.getID());
        }
    }

    private void generateClients(Set<StateTreeProtocol> datacenters) {
        int totalClients = 0;
        for (int localityDistance = 0; localityDistance < clientsLocalityPercentages.length;
             localityDistance++) {

            int amountLocalityClients =
                    Math.round((float) ((double) clientsLocalityPercentages[localityDistance] / 100) * maxClients);
            int amountEagerClients =
                    Math.round((float) ((double) clientEagerPercentage / 100) * amountLocalityClients);

            for (StateTreeProtocol datacenter : datacenters) {


                Map<Integer, Set<StateTreeProtocol>> neighboursOfDatacenter =
                        GroupsManager.getInstance()
                                .getNeighboursOfDatacenter(datacenter.getNodeId());
                Map<Integer, Set<StateTreeProtocol>> localityNeighboursOfDatacenter =
                        getNeighboursInsideRadius(localityDistance, neighboursOfDatacenter);
                Map<Integer, Set<DataObject>> allDataObjectsOfNeighbours =
                        getDataObjectsOfNeighbours(localityNeighboursOfDatacenter);

                Set<Client> clients = new HashSet<>();
                for (int i = 0; i < amountLocalityClients; i++) {
                    totalClients++;
                    if (i < amountEagerClients) {
                        clients.add(new Client(totalClients, true, allDataObjectsOfNeighbours, datacenter, localityDistance));
                    } else {
                        clients.add(new Client(totalClients, false, allDataObjectsOfNeighbours, datacenter, localityDistance));
                    }
                }
                datacenter.addClients(clients);
            }
        }
    }

    private Map<Integer, Set<DataObject>> getDataObjectsOfNeighbours(
            Map<Integer, Set<StateTreeProtocol>> localityNeighboursOfDatacenter) {
        Map<Integer, Set<DataObject>> result = new HashMap<>();

        Set<StateTreeProtocol> allNeighbours = localityNeighboursOfDatacenter.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        for (StateTreeProtocol datacenter : allNeighbours) {
            Map<Integer, Set<DataObject>> allDataObjectsPerLevel = datacenter.getAllDataObjectsPerLevel();
            for (Integer level : allDataObjectsPerLevel.keySet()) {
                result.computeIfAbsent(level, k -> new HashSet<>())
                        .addAll(allDataObjectsPerLevel.get(level));
            }
        }

        return result;
    }

    private Map<Integer, Set<StateTreeProtocol>> getNeighboursInsideRadius(
            int localityLevel,
            Map<Integer, Set<StateTreeProtocol>> neighboursOfDatacenter) {

        Map<Integer, Set<StateTreeProtocol>> result = new HashMap<>();
        for (int i = 0; i < localityLevel + 1; i++) {
            Set<StateTreeProtocol> neighbours = neighboursOfDatacenter.get(i);
            result.put(i, new HashSet<>(neighbours));
        }
        return result;
    }

    private void populateDatacenterLevelsToDatacenters(Set<StateTreeProtocol> datacenters) {
        for (StateTreeProtocol datacenter : datacenters) {
            Map<Integer, Set<StateTreeProtocol>> levelsToNodes =
                    GroupsManager.getInstance().getNeighboursOfDatacenter(datacenter.getNodeId());
            datacenter.setLevelsToNodes(levelsToNodes);
        }
    }

    private void generateDataObjectsForGroup(Set<StateTreeProtocol> datacentersGroup,
                                             int level, double percentage) {
        int numberObjectsToCreate = Math.round((float) (percentage / 100) * totalObjects);
        int numberObjectsPerShard = Math.round((float) numberObjectsToCreate / numberShardsPerGroup);
        Map<Integer, Set<Integer>> shardIdsToKeys = new HashMap<>();

        Set<DataObject> result = new HashSet<>();
        Set<Long> nodesGroupIds = datacentersGroup.stream()
                .map(StateTreeProtocol::getNodeId)
                .collect(Collectors.toSet());

        // System.out.println("Group: " + nodesGroupIds);
        int shardKeysCounter = 0;
        for (int i = 0; i < numberObjectsToCreate; i++) {
            DataObject dataObject = new DataObject(level, nodesGroupIds, i, dataObjectCounter++);
            result.add(dataObject);
            shardIdsToKeys.computeIfAbsent(currentShardId, k -> new HashSet<>())
                    .add(dataObject.getKey());
            shardKeysCounter++;
            if (shardKeysCounter == numberObjectsPerShard) {
                shardKeysCounter = 0;
                currentShardId++;
            }
        }
        for (StateTreeProtocol datacenter : datacentersGroup) {
            datacenter.addDataObjectsToLevel(result, level);
            GroupsManager.getInstance().addDataObjects(datacenter, result, level);
        }

        for (int shardId : shardIdsToKeys.keySet()) {
            GroupsManager.getInstance().addShardMapping(shardId, shardIdsToKeys.get(shardId));
        }

        splitShardsAmongstDatacenters(shardIdsToKeys.keySet(), datacentersGroup);
    }

    private void splitShardsAmongstDatacenters(Set<Integer> shardIds,
                                               Set<StateTreeProtocol> datacentersGroup) {
        List<StateTreeProtocol> listOfDcs = new ArrayList<>(datacentersGroup);

        int recursiveCounter = 0;
        for (Integer shardId : shardIds) {
            StateTreeProtocol master = listOfDcs.get(recursiveCounter);
            GroupsManager.getInstance().addShardMaster(shardId, master, datacentersGroup);
            recursiveCounter++;
            if (recursiveCounter == listOfDcs.size()) {
                recursiveCounter = 0;
            }
        }
    }

    private Set<StateTreeProtocol> getDatacenters() {
        Set<StateTreeProtocol> result = new HashSet<>();

        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            result.add((StateTreeProtocol) node.getProtocol(tree));
        }

        return result;
    }


    private void debugPrintStatus(Set<StateTreeProtocol> datacenters) {
        for (StateTreeProtocol datacenter : datacenters) {
            System.out.println("Printing info for node " + datacenter.getNodeId());
            for (int level = 0; level < levelsPercentage.length; level++) {
                Set<StateTreeProtocol> levelsToNodes = datacenter.getLevelsToNodes(level);
                System.out.println("Level " + level + ": " + levelsToNodes.stream()
                        .map(StateTreeProtocol::getNodeId)
                        .map(Object::toString)
                        .sorted()
                        .collect(Collectors.joining("-")));
                for (DataObject dataObject : datacenter.getDataObjectsFromLevel(level)) {
                    System.out.println("dataobject: " + dataObject.getDebugInfo());
                }
            }
            System.out.println();
        }
    }


}
