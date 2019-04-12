package example.capstone;

import example.common.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
    // Fields
    // ------------------------------------------------------------------------

    private final int datacenter;
    private final int broker;

    private final int[] levelsPercentage;
    private final int[] clientsLocalityPercentages;


    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    public InitTreeProtocol(String prefix) {
        datacenter = Configuration.getPid("datacenter");
        broker = Configuration.getPid("broker");

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
        // Generate first 1 level (local) data objects, then 2 levels (regional)
        // then 3 levels (country) ...
        initStateTreeProtocol();
        Set<StateTreeProtocol> datacenters = getDatacenters();

        populateDatacenterLevelsToDatacenters(datacenters);

        generateDataObjects(datacenters);
        generateClients(datacenters);

        GroupsManager.getInstance().fillSRT();

        // debugPrintStatus(datacenters);

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
                datacenterGroup.addAll(datacenter.getLevelsToNodes(level));
                seenDatacenters.addAll(datacenterGroup);

                counter = generateDataObjectsForGroup(datacenterGroup, level, levelsPercentage[level], counter);
            }
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

                Set<Client> clients = new HashSet<>();
                for (int i = 0; i < amountLocalityClients; i++) {
                    totalClients++;
                    if (i < amountEagerClients) {
                        clients.add(new Client(totalClients, true, allDataObjectsOfNeighbours, datacenter, localityDistance, GroupsManager.getInstance()));
                    } else {
                        clients.add(new Client(totalClients, false, allDataObjectsOfNeighbours, datacenter, localityDistance, GroupsManager.getInstance()));
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
            datacenter.addDataObjectsToLevel(result, level);
            GroupsManager.getInstance().addDataObjects(datacenter, result, level);
        }

        return counter;
    }

    private void initStateTreeProtocol() {
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            DatacenterProtocol dc = (DatacenterProtocol) node.getProtocol(this.datacenter);
            BrokerProtocol bp = (BrokerProtocol) node.getProtocol(this.broker);
            bp.setNodeId(node.getID());
            dc.init(node.getID());
        }
    }

    private Set<StateTreeProtocol> getDatacenters() {
        Set<StateTreeProtocol> result = new HashSet<>();
        for (int i = 0; i < Network.size(); i++) {
            ProtocolMapperInit.Type type = ProtocolMapperInit.nodeType.get((long) i);
            if (type == ProtocolMapperInit.Type.DATACENTER) {
                result.add((StateTreeProtocol) Network.get(i).getProtocol(datacenter));
            }
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
