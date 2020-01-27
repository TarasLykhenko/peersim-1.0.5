package example.saturn;

import example.common.PointToPointTransport;
import example.common.datatypes.DataObject;
import example.saturn.components.TreeHelper;
import javafx.util.Pair;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static example.common.Settings.*;

public class InitTreeProtocol implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------

    private static final String PAR_TREE_PROT = "tree_protocol";
    private static final String N_CLIENTS = "n_clients";


    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final int tree;

    private final int[] levelsPercentage;
    private final int[] clientsLocalityPercentages;
    private Random rand = new Random();
    static AtomicLong ID = new AtomicLong(0);




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

        if (PRINT_INFO) {
            debugPrintStatus(datacenters);
        }

        return false;
    }

    private void generateDataObjects(Set<StateTreeProtocol> datacenters) {
        int counter = 0;

        for (int level = 0; level < levelsPercentage.length; level++) {
            Set<StateTreeProtocol> seenDatacenters = new HashSet<>();

            for (StateTreeProtocol datacenter : datacenters) {
                if (seenDatacenters.contains(datacenter)) {
                    continue;
                }

                Set<StateTreeProtocol> datacenterGroup = new HashSet<>();
                datacenterGroup.add(datacenter);
                //datacenterGroup.addAll(datacenter.getLevelsToNodes(level));
                seenDatacenters.addAll(datacenterGroup);

                counter = generateDataObjectsForGroup(datacenterGroup, level, levelsPercentage[level], counter);
            }
        }
    }

    private void initStateTreeProtocol() {

        Pair<Integer, Integer> range = new Pair<>(2, 2);

        Node node = Network.get((int)ID.getAndIncrement());
        StateTreeProtocolInstance treeProtocol = (StateTreeProtocolInstance) node.getProtocol(tree);
        treeProtocol.setNodeId(node.getID());
       createTreeHelper(treeProtocol, 5, range);

       ((PointToPointTransport) node.getProtocol(Configuration.getPid("transport")))
                .setGroupsManager(GroupsManager.getInstance());

    }


    //range <min, max>
    public void createTreeHelper(StateTreeProtocolInstance parent, int depth, Pair<Integer,Integer> range){

        if (depth <= 0) return;

        depth--;
        int leafsRange = rand.nextInt((range.getValue() - range.getKey()) + 1) + range.getKey();

        for (int i = 0; i < leafsRange; i++ ){
            Node node = Network.get((int)ID.getAndIncrement());
            StateTreeProtocolInstance treeProtocol = (StateTreeProtocolInstance) node.getProtocol(tree);
            treeProtocol.setNodeId(node.getID());

            ((PointToPointTransport) node.getProtocol(Configuration.getPid("transport")))
                    .setGroupsManager(GroupsManager.getInstance());
            parent.addChild(treeProtocol);
            createTreeHelper(treeProtocol, depth, range);
        }

    }

    private void generateClients(Set<StateTreeProtocol> datacenters) {
        int totalClients = 0;
        int totalNumberClients = Integer.parseInt(Configuration.getString(N_CLIENTS));
        int numberOfClient = totalNumberClients / datacenters.size();
        for (StateTreeProtocol datacenter : datacenters) {
            Set<Client> clients = new HashSet<>();
            for (int i = 0; i < numberOfClient; i++) {
                totalClients++;
                Client c = new Client(totalClients, (StateTreeProtocolInstance) datacenter);
                clients.add(c);
            }
            datacenter.addClients(clients);
        }
    }

    private Map<Integer, Set<DataObject>> getDataObjectsOfNeighbours(
            Map<Integer, Set<StateTreeProtocol>> localityNeighboursOfDatacenter) {
        Map<Integer, Set<DataObject>> result = new HashMap<>();

        Set<StateTreeProtocol> allNeighbours = localityNeighboursOfDatacenter.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        for (StateTreeProtocol datacenter : allNeighbours) {
            //Map<Integer, Set<DataObject>> allDataObjectsPerLevel = datacenter.getAllDataObjectsPerLevel();
           /* for (Integer level : allDataObjectsPerLevel.keySet()) {
                result.computeIfAbsent(level, k -> new HashSet<>())
                        .addAll(allDataObjectsPerLevel.get(level));
            }*/
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
           // datacenter.setLevelsToNodes(levelsToNodes);
        }
    }

    private int generateDataObjectsForGroup(Set<StateTreeProtocol> datacentersGroup,
                                            int level, double percentage, int counter) {
        int numberObjectsToCreate = Math.round((float) (percentage / 100) * TOTAL_OBJECTS_PER_DATACENTER);
        Set<DataObject> result = new HashSet<>();
        Set<Long> nodesGroupIds = datacentersGroup.stream()
                .map(StateTreeProtocol::getNodeId)
                .collect(Collectors.toSet());
        // System.out.println("Group: " + nodesGroupIds);
        for (int i = 0; i < numberObjectsToCreate; i++) {
            result.add(new DataObject(level, nodesGroupIds, i, counter++));
        }
        for (StateTreeProtocol datacenter : datacentersGroup) {
            //datacenter.addDataObjectsToLevel(result, level);
            GroupsManager.getInstance().addDataObjects(datacenter, result, level);
        }

        return counter;
    }

    private Set<StateTreeProtocol> getDatacenters() {
        Set<StateTreeProtocol> result = new HashSet<>();

        for (int i = 0; i < ID.get(); i++) {
            Node node = Network.get(i);
            result.add((StateTreeProtocol) node.getProtocol(tree));
        }

        return result;
    }


    private void debugPrintStatus(Set<StateTreeProtocol> datacenters) {
        for (StateTreeProtocol datacenter : datacenters) {
            System.out.println("Printing info for node " + datacenter.getNodeId());
            for (int level = 0; level < levelsPercentage.length; level++) {
                //Set<StateTreeProtocol> levelsToNodes = datacenter.getLevelsToNodes(level);
               /* System.out.println("Level " + level + ": " + levelsToNodes.stream()
                        .map(StateTreeProtocol::getNodeId)
                        .map(Object::toString)
                        .sorted()
                        .collect(Collectors.joining("-")));
               /* for (DataObject dataObject : datacenter.getDataObjectsFromLevel(level)) {
                    System.out.println("dataobject: " + dataObject.getDebugInfo());
                }*/
            }
            System.out.println();
        }
    }


}
