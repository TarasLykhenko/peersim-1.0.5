package example.saturn;

import example.common.PointToPointTransport;
import example.common.datatypes.DataObject;
import example.saturn.auxiliar.TreeHelper;
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
    private static final String N_OBJECTS = "n_objects";


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

        StateTreeProtocolInstance root = initStateTreeProtocol(2, 2, 2);
        TreeHelper.printTree(root);

        int[][] latencies =  generateLatencies();
        TreeHelper.printLatencies(latencies);


        // Generate first 1 level (local) data objects, then 2 levels (regional)
        // then 3 levels (country) ...
        Set<StateTreeProtocol> datacenters = getDatacenters();


        generateDataObjects(datacenters);
        generateClients(datacenters);

        if (PRINT_INFO) {
            debugPrintStatus(datacenters);
        }

        return false;
    }

    private int[][] generateLatencies(){

        int numberOfNodes = (int)ID.get();
        int[][] latencies = new int [numberOfNodes][numberOfNodes];

        for (int i = 0; i < numberOfNodes; i++) {
            Node node = Network.get(i);
            StateTreeProtocolInstance treeProtocol = (StateTreeProtocolInstance) node.getProtocol(tree);
            TreeHelper.generateLatencies(treeProtocol, latencies);
        }


        return latencies;
    }

    private void generateDataObjects(Set<StateTreeProtocol> datacenters) {

        int totalDataObjects = Integer.parseInt(Configuration.getString(N_OBJECTS));

        for (StateTreeProtocol datacenter : datacenters) {
            datacenter.generateKeys(totalDataObjects);
        }

    }

    private StateTreeProtocolInstance initStateTreeProtocol(int depth, int rangeMin, int rangeMax) {

        Pair<Integer, Integer> range = new Pair<>(rangeMin, rangeMax);

        Node node = Network.get((int)ID.getAndIncrement());
        StateTreeProtocolInstance treeProtocol = (StateTreeProtocolInstance) node.getProtocol(tree);
        treeProtocol.setNodeId(node.getID());
        createTreeHelper(treeProtocol, depth, range);

        ((PointToPointTransport) node.getProtocol(Configuration.getPid("transport")))
                .setGroupsManager(GroupsManager.getInstance());

        return treeProtocol;

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
            treeProtocol.setParent(parent);

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
