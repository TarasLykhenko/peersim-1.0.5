package example.occult;

import example.common.PointToPointTransport;
import example.common.Settings;
import example.common.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static example.common.Settings.CLIENTS_PER_DATACENTER;
import static example.common.Settings.CLIENT_EAGER_PERCENTAGE;
import static example.common.Settings.CLIENT_LOCALITY_PERCENTAGE;
import static example.common.Settings.LEVELS_PERCENTAGE;
import static example.common.Settings.TOTAL_OBJECTS_PER_DATACENTER;

public class InitTreeProtocol implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------

    private static final String PAR_TREE_PROT = "tree_protocol";

    private static final String PAR_NUMBER_OBJECTS_PER_SHARD = "objects_per_shard";
    private static final String PAR_OCCULT_TYPE = "occult_type";

    private static final String OCCULT_NO_COMPRESSION = "no_compression";
    private static final String OCCULT_TEMPORAL_COMPRESSION = "temporal_compression";


    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final int tree;
    private final int[] levelsPercentage;
    private final int[] clientsLocalityPercentages;
    private final int numberObjectsPerShard;
    private final String clientType;


    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    public InitTreeProtocol(String prefix) {
        tree = Configuration.getPid(prefix + "." + PAR_TREE_PROT);

        levelsPercentage = Arrays.stream(LEVELS_PERCENTAGE
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();

        clientsLocalityPercentages = Arrays.stream(CLIENT_LOCALITY_PERCENTAGE
                .replace("[", "")
                .replace("]", "")
                .split(","))
                .mapToInt(Integer::parseInt)
                .toArray();

        numberObjectsPerShard = Configuration.getInt(PAR_NUMBER_OBJECTS_PER_SHARD);
        clientType = Configuration.getString(PAR_OCCULT_TYPE);
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

        //debugPrintStatus(datacenters);

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
            ((PointToPointTransport) node.getProtocol(Configuration.getPid("transport")))
                    .setGroupsManager(GroupsManager.getInstance());
        }
    }

    private void generateClients(Set<StateTreeProtocol> datacenters) {
        int totalClients = 0;
        for (int localityDistance = 0; localityDistance < clientsLocalityPercentages.length;
             localityDistance++) {

            int amountLocalityClients =
                    Math.round((float) ((double) clientsLocalityPercentages[localityDistance] / 100) * CLIENTS_PER_DATACENTER);
            int amountEagerClients =
                    Math.round((float) ((double) CLIENT_EAGER_PERCENTAGE / 100) * amountLocalityClients);

            for (StateTreeProtocol datacenter : datacenters) {


                Map<Integer, Set<StateTreeProtocol>> neighboursOfDatacenter =
                        GroupsManager.getInstance()
                                .getNeighboursOfDatacenter(datacenter.getNodeId());
                Map<Integer, Set<StateTreeProtocol>> localityNeighboursOfDatacenter =
                        getNeighboursInsideRadius(localityDistance, neighboursOfDatacenter);
                Map<Integer, Set<DataObject>> allDataObjectsOfNeighbours =
                        getDataObjectsOfNeighbours(localityNeighboursOfDatacenter);

                Set<OccultClientInterface> clients = new HashSet<>();
                for (int i = 0; i < amountLocalityClients; i++) {
                    totalClients++;
                    if (i < amountEagerClients) {
                        OccultClientInterface newClient = clientFactory(totalClients, true, allDataObjectsOfNeighbours, datacenter, localityDistance, GroupsManager.getInstance());
                        clients.add(newClient);
                    } else {
                        OccultClientInterface newClient = clientFactory(totalClients, false, allDataObjectsOfNeighbours, datacenter, localityDistance, GroupsManager.getInstance());
                        clients.add(newClient);
                    }
                }
                datacenter.addClients(clients);
            }
        }
    }

    private OccultClientInterface clientFactory(int totalClients,
                                                boolean isEager,
                                                Map<Integer, Set<DataObject>> allDataObjectsOfNeighbours,
                                                StateTreeProtocol datacenter,
                                                int localityDistance, GroupsManager groupsManager) {
        if (clientType.equals(OCCULT_NO_COMPRESSION)) {
            return new example.occult.no_compression.Client(totalClients, isEager, allDataObjectsOfNeighbours, datacenter, localityDistance, groupsManager);
        } else if (clientType.equals(OCCULT_TEMPORAL_COMPRESSION)) {
            return new example.occult.temporal_compression.Client(totalClients, isEager, allDataObjectsOfNeighbours, datacenter, localityDistance, groupsManager);
        } else {
            throw new RuntimeException("Wrong occult type");
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
        int numberObjectsToCreate = Math.round((float) (percentage / 100) * TOTAL_OBJECTS_PER_DATACENTER);
        if (Settings.PRINT_INFO) {
            System.out.println("Generating " + numberObjectsToCreate + " objects for group.");
        }
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
            if (shardKeysCounter == this.numberObjectsPerShard - 1) {
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
        currentShardId++;
    }

    private void splitShardsAmongstDatacenters(Set<Integer> shardIds,
                                               Set<StateTreeProtocol> datacentersGroup) {
        List<StateTreeProtocol> listOfDcs = new ArrayList<>(datacentersGroup);

        int recursiveCounter = 0;
        String group = datacentersGroup.stream().map(StateTreeProtocol::getNodeId).map(Object::toString).sorted().collect(Collectors.joining(" "));
       // System.out.println("Splitting! (Group: " + group + ")");
        for (Integer shardId : shardIds) {
            StateTreeProtocol master = listOfDcs.get(recursiveCounter);
         //   System.out.println("Shard is " + shardId + ", master is " + master.getNodeId());
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
