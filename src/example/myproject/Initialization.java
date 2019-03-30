package example.myproject;

import example.myproject.datatypes.NodePath;
import example.myproject.datatypes.PathMessage;
import example.myproject.server.BackendInterface;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.ExtendedRandom;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Initialization implements Control {

    private static final String PAR_LINKABLE = "linkable"; // TODO bem
    private static final String PAR_BACKEND = "backend";
    private static final String PAR_DISTINCT_GROUPS = "distinct_groups";
    private static final String PAR_NUMBER_GROUPS_PER_NODE = "number_groups_per_node";

    private final int linkablePid;
    private final int backendPid;
    private final int distinctGroups;
    private final int numberGroupsPerNode;

    public static Map<Long, Set<Integer>> nodeToGroups = new LinkedHashMap<>();
    public static Map<Integer, Set<Long>> groupsToMembers = new HashMap<>();
    public static Map<Integer, Set<Long>> groupsToMembersAndForwarders = new HashMap<>();
    public static Map<Long, NodePath> pathsToPathLongs = new HashMap<>();
    public static Map<Long, BackendInterface> servers = new LinkedHashMap<>();

    public Initialization(String prefix) {
        this.linkablePid = Configuration.getPid(prefix + "." + PAR_LINKABLE);
        this.backendPid = Configuration.getPid(PAR_BACKEND);
        this.distinctGroups = Configuration.getInt(PAR_DISTINCT_GROUPS);
        this.numberGroupsPerNode = Configuration.getInt(PAR_NUMBER_GROUPS_PER_NODE);
    }

    /**
     * Generate the neighbourhood of each node.
     * The algorithm is:
     * 1) For each node, calculate every distinct path up to X distance
     *
     * How can these distinct paths be found? There is a distinct path if
     * for any given neighbour, that neighbour's degree is higher than 1
     * (excluding the original node)
     *
     * @return
     */
    @Override
    public boolean execute() {
        Long startTime = System.currentTimeMillis();

        setBackendIds();
        generateGlobalPaths();
        discoverPathsForEachNode();
        generateSRTsForEachNode();
        startActiveConnections();

        discoverSRTsOfNeighbours();

        Long endTime = System.currentTimeMillis();
        System.out.println("Init took " + (endTime - startTime));
        return false;
    }

    private void startActiveConnections() {
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);

            Linkable linkable = (Linkable) node.getProtocol(linkablePid);
            for (int j = 0; j < linkable.degree(); j++) {
                Node neighbour = linkable.getNeighbor(j);
                BackendInterface neighbourServer = nodeToBackend(neighbour);
                neighbourServer.startActiveConnection(node.getID());
            }
        }
    }

    private void setBackendIds() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);
            BackendInterface server = nodeToBackend(node);
            server.init(node.getID());
            servers.put(server.getId(), server);
        }
    }

    /**
     * Maps the path of a src to a dst.
     *
     * First long is src
     * Second long is dst
     * List is the path itself.
     */
    private Map<Long, Map<Long, List<Node>>> globalPaths = new HashMap<>();


    private void generateGlobalPaths() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);

            BackendInterface server = nodeToBackend(node);

            Set<List<Node>> result = new HashSet<>();
            List<Node> currentPath = new ArrayList<>();
            currentPath.add(node);
            getPaths(currentPath, result, 0, Integer.MAX_VALUE);

            for (List<Node> path : result) {
                Node endNode = Utils.getLastEntry(path);
                globalPaths.computeIfAbsent(node.getID(), k -> new HashMap<>())
                        .put(endNode.getID(), path);
            }
        }

        /*
        System.out.println("DEBUG GLOBAL PATHS");
        for (Long src : globalPaths.keySet()) {
            System.out.println("SRC: " + src);
            Map<Long, List<Node>> targetAndPath = globalPaths.get(src);
            for (Long dst : targetAndPath.keySet()) {
                List<Node> path = targetAndPath.get(dst);
                String pathString = path.stream().map(Node::getID).map(Objects::toString).collect(Collectors.joining(" "));
                System.out.println("    DST: " + dst + " | path: " + pathString);
            }
        }
        */
    }

    private void discoverPathsForEachNode() {
        long pathId = 100_000;

        // First generate the Delta + 1 paths for each node
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);
            BackendInterface server = nodeToBackend(node);

            Set<List<Node>> result = new HashSet<>();
            List<Node> currentPath = new ArrayList<>();
            currentPath.add(node);
            getPaths(currentPath, result, 0, Utils.DELTA + 1);

            if (Utils.DEBUG_VERY_V) {
                System.out.println("Printing paths for " + node.getID());
            }

            Set<PathMessage> messagesToSpread = new HashSet<>();
            for (List<Node> path : result) {
                pathId++;
                NodePath nodePath = new NodePath(path, pathId);
                if (Utils.DEBUG_VERY_V) {
                    nodePath.printLn(">> " + nodePath.id + " ");
                }
                pathsToPathLongs.put(pathId, nodePath);
                messagesToSpread.add(new PathMessage(pathId, nodePath, 0, server.getId()));
            }

            for (PathMessage pathMessage : messagesToSpread) {
                server.receivePath(pathMessage);
            }
        }
    }

    private void generateSRTsForEachNode() {
        // Step 1: Add groups each node will be interested in
        Map<Long, Set<Integer>> groupsInfo = ScenarioReader.getInstance().getGroupsInfo();
        if (groupsInfo == null) {
            generateRandomGroupsForNodes();
        } else {
            generateGroupsFromFile(groupsInfo);
        }

        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            BackendInterface server = nodeToBackend(node);

            Set<Integer> groups = server.getGroups();
            List<Node> path = new ArrayList<>();
            path.add(node);
            spreadGroups(groups, path);
        }

        for (BackendInterface server : servers.values()) {
            System.out.println("Groups of " + server.getId() + ": " + server.getCopyOfSRT());
        }
    }

    private void spreadGroups(Set<Integer> groups, List<Node> currentPath) {
        Node beforeLast = currentPath.size() == 1 ? null : currentPath.get(currentPath.size() - 2);
        Node lastNode = Utils.getLastEntry(currentPath);
        Set<Node> neighboursExcludingSource = Utils.getNeighboursExcludingSource(lastNode, beforeLast);
        for (Node node : neighboursExcludingSource) {
            BackendInterface server = nodeToBackend(node);

            for (Integer group : groups) {
                if (server.belongsToGroup(group)) {
                    addIntermediateForwarders(currentPath, group);
                }
            }

            List<Node> newPath = new ArrayList<>(currentPath);
            newPath.add(node);
            spreadGroups(groups, newPath);
        }
    }

    private void addIntermediateForwarders(List<Node> path, int group) {
        if (Utils.DEBUG_VERY_V) {
            System.out.println("Path is : " +
                    path.stream().map(Node::getID).map(Object::toString).collect(Collectors.joining("-")));
        }

        for (Node node : path) {
            BackendInterface server = nodeToBackend(node);
            if (!server.belongsToGroup(group)) {
                if (Utils.DEBUG_VERY_V) {
                    System.out.println("adding " + server.getId() + " as forwarder for " + group);
                }
                server.setForwarderOfGroup(group);
                nodeToGroups.get(server.getId()).add(group);
            }
        }
    }

    private void generateRandomGroupsForNodes() {
        ExtendedRandom random = CommonState.r;
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            for (int i = 0; i < numberGroupsPerNode; i++) {
                int group = random.nextInt(distinctGroups);
                BackendInterface server = nodeToBackend(Network.get(nodeIdx));
                server.addGroup(group);

                groupsToMembers.computeIfAbsent(group, k -> new HashSet<>())
                        .add(server.getId());
                nodeToGroups.computeIfAbsent(server.getId(), k -> new HashSet<>())
                        .add(group);
            }
        }
    }

    private void generateGroupsFromFile(Map<Long, Set<Integer>> nodesToGroups) {
        for (Long nodeId : nodesToGroups.keySet()) {
            Set<Integer> groups = nodesToGroups.get(nodeId);

            nodeToGroups.put(nodeId, groups);

            for (Integer group : groups) {
                groupsToMembers.computeIfAbsent(group, k -> new HashSet<>())
                        .add(nodeId);
                BackendInterface server = Utils.nodeToBackend(Network.get(Math.toIntExact(nodeId)));
                server.addGroup(group);
            }
        }
    }

    private void discoverSRTsOfNeighbours() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);

            BackendInterface server = nodeToBackend(node);

            Set<NodePath> paths = server.getNeighbourhood();
            Set<Node> distinctNeighbours = paths.stream()
                    .map(path -> path.path)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            for (Node neighbour : distinctNeighbours) {
                BackendInterface neighbourServer = nodeToBackend(neighbour);

                server.addNeighbourSRT(
                        neighbourServer.getId(),
                        neighbourServer.getCopyOfSRT());
            }

            /*
            System.out.println("I am " + nodeIdx);
            for (Node n : distinctNeighbours) {
                System.out.println(">> " + n.getID());
            }
            */
        }
    }

    private void getPaths(List<Node> currentPath, Set<List<Node>> result,
                          int currentDistance, int maxDistance) {
        // End of path
        if (currentDistance == maxDistance) {
            result.add(currentPath);
            System.out.println("Got to max distance of " + maxDistance);
            return;
        }
        Node lastNode = Utils.getLastEntry(currentPath);
        Node nodeBeforeLastNode;
        if (currentPath.size() <= 1) {
            nodeBeforeLastNode = null;
        } else {
            nodeBeforeLastNode = currentPath.get(currentPath.size() - 2);
        }
        Set<Node> nextNeighbours = Utils.getNeighboursExcludingSource(lastNode, nodeBeforeLastNode);

        result.add(currentPath);
        for (Node nextNode : nextNeighbours) {
            List<Node> nextPath = new ArrayList<>(currentPath);
            nextPath.add(nextNode);
            getPaths(nextPath, result, currentDistance + 1, maxDistance);
        }
    }

    private BackendInterface nodeToBackend(Node node) {
        return (BackendInterface) node.getProtocol(backendPid);
    }
}
