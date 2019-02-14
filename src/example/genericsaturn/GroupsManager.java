package example.genericsaturn;

import example.genericsaturn.datatypes.DataObject;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.graph.Graph;
import peersim.util.ExtendedRandom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupsManager {

    private static final String PAR_TYPE = "type";
    private static final String PAR_TREE = "tree";
    private static final String PAR_LEVELS = "levels";
    private final int tree;
    private final int type;
    private final int levels;

    private ExtendedRandom random = CommonState.r;

    private Map<Long, Map<Integer, Set<StateTreeProtocol>>> nodeToLevelNeighbours = new HashMap<>();

    private Map<Integer, Set<DataObject>> levelsToAllDataObjects = new HashMap<>();
    private Map<StateTreeProtocol, Map<Integer, Set<DataObject>>> dataCenterDataObjects = new HashMap<>();

    private Map<Integer, DataObject> idToObjects = new HashMap<>();
    private Map<StateTreeProtocol, Set<DataObject>> datacenterToObjects = new HashMap<>();

    public Map<Integer, Set<DataObject>> getLevelsToAllDataObjects() {
        return levelsToAllDataObjects;
    }


    private GroupsManager() {
        type = Configuration.getPid(PAR_TYPE);
        tree = Configuration.getPid(PAR_TREE);
        levels = Configuration.getInt(PAR_LEVELS);
    }
    private static GroupsManager groupsManager;

    public DataObject getRandomDataObject() {
        int level = random.nextInt(levels);
        Set<DataObject> dataObjects = levelsToAllDataObjects.get(level);
        return dataObjects.stream().skip(random.nextInt(dataObjects.size())).findFirst().get();
    }


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
        System.out.println();
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
        levelsToAllDataObjects
                .computeIfAbsent(level, k -> new HashSet<>())
                .addAll(result);

        for (DataObject obj : result) {
            idToObjects.put(obj.getTotalCounter(), obj);
        }
        datacenterToObjects.computeIfAbsent(datacenter, k -> new HashSet<>())
                .addAll(result);
    }

    public Set<DataObject> getDataObjectsFromDatacenter(StateTreeProtocol datacenter) {
        return datacenterToObjects.get(datacenter);
    }

    public boolean datacenterContainsObject(StateTreeProtocol datacenter, int key) {
        if (!datacenterToObjects.containsKey(datacenter)) {
            // TODO SUPER HARDOCDED, ATM ISTO SIGNIFICA QUE TODOS OS BROKERS RECEBEM
            // TODO TODAS AS CENAS.
            return true;
        }
        DataObject dataObject = idToObjects.get(key);
        return datacenterToObjects.get(datacenter).contains(dataObject);
    }
}
