package example.capstone;

import example.common.BasicStateTreeProtocol;
import example.common.GroupsManagerInterface;
import example.common.Settings;
import example.common.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.graph.Graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static example.capstone.ProtocolMapperInit.*;
import static example.capstone.ProtocolMapperInit.nodeType;

public class GroupsManager implements GroupsManagerInterface {

    private static final String PAR_LEVELS = "levels";
    private static final String PAR_ROOT_NODE = "root";

    private TreeOverlay treeOverlay;
    private final int datacenter;
    private final int levels;
    private final int root;

    private Map<Long, Map<Integer, Set<DataObject>>> dataCenterIdsDataObjects = new HashMap<>();
    private Map<Long, Map<Integer, Set<Long>>> exclusiveNodeToLevelNeighbourIds;

    private Map<Long, Map<Integer, Set<StateTreeProtocol>>> nodeToLevelNeighbours = new HashMap<>();

    private Map<Integer, Set<DataObject>> levelsToAllDataObjects = new HashMap<>();
    private Map<StateTreeProtocol, Map<Integer, Set<DataObject>>> dataCenterDataObjects = new HashMap<>();

    private Map<Integer, DataObject> idToObjects = new HashMap<>();
    private Map<StateTreeProtocol, Set<DataObject>> datacenterToObjects = new HashMap<>();

    /**
     * Maps the nodeId to the keys of the objects it contains
     */
    private Map<Long, Set<Integer>> subscriptionRoutingTable = new HashMap<>();

    private GroupsManager() {
        datacenter = Configuration.getPid("datacenter");
        levels = Configuration.getInt(PAR_LEVELS);
        root = Configuration.getInt(PAR_ROOT_NODE);
    }

    private static GroupsManager groupsManager;

    public static GroupsManager getInstance() {
        if (groupsManager == null) {
            groupsManager = new GroupsManager();
        }
        return groupsManager;
    }

    // Small cheat, literally CBA. Need to store this to be used later
    private Graph graph;

    // Formula: (Level - 1) * 2
    public void populate(Graph graph) {
        this.graph = graph;
        // 1) Generate data for each datacenter
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = (Node) graph.getNode(nodeIdx);

            if (nodeIsDatacenter(node)) {
                Map<Integer, Set<StateTreeProtocol>> nodeNeighbours = generateNeighbours(graph, nodeIdx);
                nodeToLevelNeighbours.put(node.getID(), nodeNeighbours);
            }
        }

        // 2) Generate the tree overlay
        treeOverlay = new TreeOverlay(graph, root);
    }

    void fillSRT() {
        generateSRT(graph);
    }

    private void generateSRT(Graph graph) {
        Set<Long> leafs = new HashSet<>();
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = (Node) graph.getNode(nodeIdx);

            if (nodeIsDatacenter(node)) {
                leafs.add(node.getID());
            }
        }

        Set<Long> alteredNodes = new HashSet<>(leafs);
        while (!alteredNodes.isEmpty()) {
            Set<Long> nodesToUpdateParent = new HashSet<>(alteredNodes);
            alteredNodes.clear();
            for (Long nodeId : nodesToUpdateParent) {
                Long parentId = treeOverlay.getParent(nodeId);
                if (parentId == null) {
                    continue;
                }
                alteredNodes.add(parentId);

                Set<Integer> keys = subscriptionRoutingTable
                        .computeIfAbsent(nodeId, k -> new HashSet<>());
                subscriptionRoutingTable
                        .computeIfAbsent(parentId, k -> new HashSet<>())
                        .addAll(keys);
            }
        }
    }

    public boolean isInterested(long nodeId, int key) {
        return subscriptionRoutingTable.get(nodeId).contains(key);
    }

    public Map<Integer, Set<StateTreeProtocol>> getNeighboursOfDatacenter(Long nodeId) {
        return nodeToLevelNeighbours.get(nodeId);
    }

    private Map<Integer, Set<StateTreeProtocol>> generateNeighbours(Graph graph, Integer nodeIdx) {
        Map<Integer, Set<StateTreeProtocol>> levelsToNodes = new HashMap<>();
        Node currentNode = (Node) graph.getNode(nodeIdx);

        for (int level = 0; level < levels; level++) {
            Set<StateTreeProtocol> memberNodes = new HashSet<>();
            memberNodes.add((StateTreeProtocol) currentNode.getProtocol(datacenter));
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
                        memberNodes.add((StateTreeProtocol) node.getProtocol(datacenter));
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
        return nodeType.get(node.getID()) == Type.DATACENTER;
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
        Set<Integer> keySet = subscriptionRoutingTable
                .computeIfAbsent(datacenter.getNodeId(), k -> new HashSet<>());

        for (DataObject obj : result) {
            keySet.add(obj.getKey());
        }

        datacenterToObjects.computeIfAbsent(datacenter, k -> new HashSet<>())
                .addAll(result);
    }

    public Set<DataObject> getDataObjectsFromDatacenter(StateTreeProtocol datacenter) {
        return datacenterToObjects.get(datacenter);
    }

    TreeOverlay getTreeOverlay() {
        return treeOverlay;
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
