package example.genericsaturn;

import example.common.BasicStateTreeProtocol;
import example.common.GroupsManagerInterface;
import example.common.Settings;
import example.common.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.graph.Graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupsManager implements GroupsManagerInterface {

    private static final String PAR_TYPE = "type";
    private static final String PAR_TREE = "tree";
    private static final String PAR_LEVELS = "levels";
    private final int tree;
    private final int type;
    private final int levels;

    private Map<Long, Map<Integer, Set<DataObject>>> dataCenterIdsDataObjects = new HashMap<>();
    private Map<Long, Map<Integer, Set<Long>>> exclusiveNodeToLevelNeighbourIds;

    private Map<Long, Map<Integer, Set<StateTreeProtocol>>> nodeToLevelNeighbours = new HashMap<>();

    private Map<Integer, Set<DataObject>> levelsToAllDataObjects = new HashMap<>();
    private Map<StateTreeProtocol, Map<Integer, Set<DataObject>>> dataCenterDataObjects = new HashMap<>();

    private Map<Integer, DataObject> idToObjects = new HashMap<>();
    private Map<StateTreeProtocol, Set<DataObject>> datacenterToObjects = new HashMap<>();

    private GroupsManager() {
        type = Configuration.getPid(PAR_TYPE);
        tree = Configuration.getPid(PAR_TREE);
        levels = Configuration.getInt(PAR_LEVELS);
    }
    private static GroupsManager groupsManager;

    public static GroupsManager getInstance() {
        if (groupsManager == null) {
            groupsManager = new GroupsManager();
        }
        return groupsManager;
    }

    // Formula: (Level - 1) * 2
    public void populate(Graph graph) {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = (Node) graph.getNode(nodeIdx);

            if (nodeIsDatacenter(node)) {
                Map<Integer, Set<StateTreeProtocol>> nodeNeighbours = generateNeighbours(graph, nodeIdx);
                nodeToLevelNeighbours.put(node.getID(), nodeNeighbours);
            }
        }
        //System.out.println();
    }

    public Map<Integer, Set<StateTreeProtocol>> getNeighboursOfDatacenter(Long nodeId) {
        return nodeToLevelNeighbours.get(nodeId);
    }

    private Map<Integer, Set<StateTreeProtocol>> generateNeighbours(Graph graph, Integer nodeIdx) {
        Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
        Node currentNode = (Node) graph.getNode(nodeIdx);

        for (int level = 0; level < levels; level++) {
            Set<StateTreeProtocol> memberNodes = new HashSet<>();
            memberNodes.add((StateTreeProtocol) currentNode.getProtocol(tree));
            Set<Integer> visitedNodes = new HashSet<>();

            Set<Integer> neighboursQueue = new HashSet<>(graph.getNeighbours(nodeIdx));
            neighboursQueue.removeIf(neighbour -> nodeIsDatacenter((Node) graph.getNode(neighbour)));

            // Formula: (Level - 1) * 2
            int maximumDepth = level * 2;

            for (int currentDepth = 0; currentDepth < maximumDepth; currentDepth++) {
                Set<Integer> currentQueue = new HashSet<>(neighboursQueue);
                for (Integer neighbour : currentQueue) {
                    if (visitedNodes.contains(neighbour)) {
                        continue;
                    }
                    visitedNodes.add(neighbour);

                    Node node = (Node) graph.getNode(neighbour);
                    if (nodeIsDatacenter(node)) {
                        memberNodes.add((StateTreeProtocol) node.getProtocol(tree));
                        continue;
                    }

                    neighboursQueue.addAll(new HashSet<>(graph.getNeighbours(neighbour)));
                }
            }
            levelsToNodes.put(level, memberNodes);
        }

        return levelsToNodes;
    }


    private boolean nodeIsDatacenter(Node node) {
        TypeProtocol typeProtocol = (TypeProtocol) node.getProtocol(type);
        return typeProtocol.getType() == TypeProtocol.Type.DATACENTER;
    }

    public void addDataObjects(StateTreeProtocol datacenter, Set<DataObject> result, int level) {
        dataCenterDataObjects
                .computeIfAbsent(datacenter, k -> new HashMap<>())
                .put(level, result);
        dataCenterIdsDataObjects
                .computeIfAbsent(datacenter.getNodeId(), k -> new HashMap<>())
                .put(level, result);
        levelsToAllDataObjects
                .computeIfAbsent(level, k -> new HashSet<>())
                .addAll(result);

        for (DataObject obj : result) {
            idToObjects.put(obj.getKey(), obj);
        }
        datacenterToObjects.computeIfAbsent(datacenter, k -> new HashSet<>())
                .addAll(result);
    }

    public Set<DataObject> getDataObjectsFromDatacenter(StateTreeProtocol datacenter) {
        return datacenterToObjects.get(datacenter);
    }

    public boolean datacenterContainsObject(StateTreeProtocol datacenter, int key) {
        if (!datacenterToObjects.containsKey(datacenter)) {
            // TODO SUPER HARDCODED, ATM ISTO SIGNIFICA QUE TODOS OS BROKERS RECEBEM
            // TODO TODAS AS CENAS.
            return true;
        }
        DataObject dataObject = idToObjects.get(key);
        return datacenterToObjects.get(datacenter).contains(dataObject);
    }

    @Override
    public Map<Long, Map<Integer, Set<DataObject>>> getDataCenterIdsDataObjects() {
        return dataCenterIdsDataObjects;
    }

    @Override
    public Map<Integer, Set<Long>> getExclusiveNodeToLevelNeighbourIds(long nodeId) {
        if (exclusiveNodeToLevelNeighbourIds == null) {
            exclusiveNodeToLevelNeighbourIds = new HashMap<>();

            // Prepare for servers
            for (Long serverId : nodeToLevelNeighbours.keySet()) {
                Map<Integer, Set<StateTreeProtocol>> levelsAndNodes = nodeToLevelNeighbours.get(serverId);

                int level = 0;
                while (true) {
                    if (!levelsAndNodes.containsKey(level)) {
                        break;
                    }

                    Set<Long> levelNodes = levelsAndNodes.get(level).stream()
                            .map(BasicStateTreeProtocol::getNodeId)
                            .collect(Collectors.toSet());

                    // Make it exclusive
                    for (int i = level - 1; i >= 0; i--) {
                        Set<Long> existingLongs = exclusiveNodeToLevelNeighbourIds.get(serverId).get(i);
                        levelNodes.removeAll(existingLongs);
                    }

                    // Add relevant brokers to the level
                    for (int broker : WireTopology.brokerSources.keySet()) {
                        long brokerDC = WireTopology.brokerSources.get(broker);
                        if (levelNodes.contains(brokerDC)) {
                            // Add entry to server and add entry to broker
                            levelNodes.add((long) broker);

                            exclusiveNodeToLevelNeighbourIds
                                    .computeIfAbsent((long) broker, k -> new HashMap<>())
                                    .computeIfAbsent(level, k -> new HashSet<>())
                                    .add(serverId);
                        }
                    }

                    // Add result
                    exclusiveNodeToLevelNeighbourIds
                            .computeIfAbsent(serverId, k -> new HashMap<>())
                            .put(level, levelNodes);

                    level++;
                }
            }

            // Add connections between brokers
            for (int broker : WireTopology.brokerSources.keySet()) {
                long originDC = WireTopology.brokerSources.get(broker);
                for (int otherBroker : WireTopology.brokerSources.keySet()) {
                    if (broker == otherBroker) {
                        continue;
                    }
                    long otherDC = WireTopology.brokerSources.get(otherBroker);
                    int commonLevel = getCommonLevel(originDC, otherDC);
                    exclusiveNodeToLevelNeighbourIds
                            .computeIfAbsent((long) broker, k -> new HashMap<>())
                            .computeIfAbsent(commonLevel, k -> new HashSet<>())
                            .add((long) otherBroker);
                }
            }


            //DEBUG
            if (Settings.PRINT_INFO) {
                for (long origin : exclusiveNodeToLevelNeighbourIds.keySet()) {
                    Map<Integer, Set<Long>> levelsMap = exclusiveNodeToLevelNeighbourIds.get(origin);
                    System.out.println("ORIGIN: " + origin);
                    for (int level : levelsMap.keySet()) {
                        Set<Long> entriesOnLevel = levelsMap.get(level);
                        System.out.println("   " + level + ": " + entriesOnLevel);
                    }
                }
            }
        }

        return exclusiveNodeToLevelNeighbourIds.get(nodeId);
    }

    private int getCommonLevel(long originDC, long otherDC) {
        Map<Integer, Set<Long>> integerSetMap = exclusiveNodeToLevelNeighbourIds.get(originDC);
        for (int level : integerSetMap.keySet()) {
            Set<Long> entries = integerSetMap.get(level);
            if (entries.contains(otherDC)) {
                return level;
            }
        }
        throw new NullPointerException("?!");
    }
}
