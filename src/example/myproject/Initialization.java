package example.myproject;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Initialization implements Control {

    private static final String PAR_DELTA = "delta";
    private static final String PAR_LINKABLE = "linkable"; // TODO bem
    private static final String PAR_BACKEND = "backend";
    private static final String PAR_DISTINCT_GROUPS = "distinct_groups";
    private static final String PAR_NUMBER_GROUPS_PER_NODE = "number_groups_per_node";

    private final int delta;
    private final int linkablePid;
    private final int backendPid;
    private final int distinctGroups;
    private final int numberGroupsPerNode;

    public Initialization(String prefix) {
        this.delta = Configuration.getInt(PAR_DELTA);
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

        discoverSRTsOfNeighbours();

        Long endTime = System.currentTimeMillis();
        System.out.println("Init took " + (endTime - startTime));
        return false;
    }

    private void setBackendIds() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);
            BackendInterface server = nodeToBackend(node);
            server.init(node.getID());
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

    private Map<Integer, Set<Long>> groupsToMembers = new HashMap<>();

    private void generateGlobalPaths() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);

            BackendInterface server = nodeToBackend(node);

            Set<List<Node>> result = new HashSet<>();
            List<Node> currentPath = new ArrayList<>();
            currentPath.add(node);
            getPaths(currentPath, result, 0, Integer.MAX_VALUE, true);

            for (List<Node> path : result) {
                Node endNode = path.get(path.size() - 1);
                globalPaths.computeIfAbsent(node.getID(), k -> new HashMap<>())
                        .put(endNode.getID(), path);
            }
        }

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
    }

    private void discoverPathsForEachNode() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);

            BackendInterface server = nodeToBackend(node);

            Set<List<Node>> result = new HashSet<>();
            List<Node> currentPath = new ArrayList<>();
            currentPath.add(node);
            getPaths(currentPath, result, 0, delta + 1, false);

            System.out.println("Printing paths for " + node.getID());
            for (List<Node> nodePath : result) {
                String pathsName = nodePath.stream()
                        .map(Node::getID)
                        .map(Objects::toString)
                        .collect(Collectors.joining("-"));
                System.out.println(">> " + pathsName);
            }

            server.setNeighbourHood(result);
        }
    }

    private void generateSRTsForEachNode() {
        ExtendedRandom random = CommonState.r;
        // Step 1: Add groups each node will be interested in
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            for (int i = 0; i < numberGroupsPerNode; i++) {
                int group = random.nextInt(distinctGroups);
                BackendInterface server = nodeToBackend(Network.get(nodeIdx));
                server.addGroup(group);

                groupsToMembers.computeIfAbsent(group, k -> new HashSet<>())
                        .add(server.getId());
            }
        }

        // Step 2: Add intermediary group forwarders
        //
        // If a node A is in the middle of two nodes that
        // are interested in a group that node A is not interested,
        // node A will need to forward it
        for (Integer group : groupsToMembers.keySet()) {
            Set<Long> members = groupsToMembers.get(group);

            Set<Node> commonPath = new HashSet<>();

            for (Long src : members) {
                for (Long dst : members) {
                    List<Node> path = globalPaths.get(src).get(dst);
                    commonPath.addAll(path);
                }
            }

            for (Node node : commonPath) {
                BackendInterface server = nodeToBackend(node);
                if (!server.belongsToGroup(group)) {
                    server.setForwarderOfGroup(group);
                }
            }
        }
    }

    private void discoverSRTsOfNeighbours() {
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);

            BackendInterface server = nodeToBackend(node);

            Set<List<Node>> paths = server.getNeighbourhood();
            Set<Node> distinctNeighbours = paths.stream().flatMap(Collection::stream).collect(Collectors.toSet());

            for (Node neighbour : distinctNeighbours) {
                BackendInterface neighbourServer = nodeToBackend(neighbour);

                server.addNeighbourSRT(
                        neighbourServer.getId(),
                        neighbourServer.getCopyOfSRT());
            }

            System.out.println("I am " + nodeIdx);
            for (Node n : distinctNeighbours) {
                System.out.println(">> " + n.getID());
            }
        }
    }

    private void getPaths(List<Node> currentPath, Set<List<Node>> result,
                          int currentDistance, int maxDistance, boolean allNodes) {
        // End of path
        if (currentDistance == maxDistance) {
            result.add(currentPath);
            return;
        }
        Node lastNode = currentPath.get(currentPath.size() - 1);
        Node nodeBeforeLastNode;
        if (currentPath.size() <= 1) {
            nodeBeforeLastNode = null;
        } else {
            nodeBeforeLastNode = currentPath.get(currentPath.size() - 2);
        }
        Set<Node> nextNeighbours = getNeighboursExcludingSource(lastNode, nodeBeforeLastNode);

        if (allNodes) {
            result.add(currentPath);
            for (Node nextNode : nextNeighbours) {
                List<Node> nextPath = new ArrayList<>(currentPath);
                nextPath.add(nextNode);
                getPaths(nextPath, result, currentDistance + 1, maxDistance, allNodes);
            }
        } else {
            if (nextNeighbours.isEmpty()) {
                // End of path
                result.add(currentPath);
            } else {
                for (Node nextNode : nextNeighbours) {
                    List<Node> nextPath = new ArrayList<>(currentPath);
                    nextPath.add(nextNode);
                    getPaths(nextPath, result, currentDistance + 1, maxDistance, allNodes);
                }
            }
        }

    }

    private Set<Node> getNeighboursExcludingSource(Node currentNode, Node sourceNode) {
        Linkable linkable = (Linkable)
                currentNode.getProtocol(linkablePid);
        Set<Node> neighbours = new HashSet<>();
        for (int i = 0; i < linkable.degree(); i++) {
            neighbours.add(linkable.getNeighbor(i));
        }
        neighbours.remove(sourceNode);

        return neighbours;
    }

    private BackendInterface nodeToBackend(Node node) {
        return (BackendInterface) node.getProtocol(backendPid);
    }
}
