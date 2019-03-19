package example.myproject;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.NodePath;
import example.myproject.server.BackendInterface;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.ExtendedRandom;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
    private static final String PAR_GROUPS_CONFIG = "groups-config";

    private final int delta;
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
            getPaths(currentPath, result, 0, Integer.MAX_VALUE, true);

            for (List<Node> path : result) {
                Node endNode = path.get(path.size() - 1);
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
        for (int nodeIdx = 0; nodeIdx < Network.size(); nodeIdx++) {
            Node node = Network.get(nodeIdx);

            BackendInterface server = nodeToBackend(node);

            Set<List<Node>> result = new HashSet<>();
            List<Node> currentPath = new ArrayList<>();
            currentPath.add(node);
            getPaths(currentPath, result, 0, delta + 1, true);

            System.out.println("Printing paths for " + node.getID());
            for (List<Node> path : result) {
                pathId++;
                NodePath nodePath = new NodePath(path, pathId);
                nodePath.printLn(">> " + nodePath.id + " ");
                server.setNeighbourhoodAndPathId(nodePath, pathId);
                pathsToPathLongs.put(pathId, nodePath);
                for (Node neighbourNode : nodePath.path) {
                    BackendInterface neighbourServer = nodeToBackend(neighbourNode);
                    neighbourServer.addPathIdMapping(nodePath, pathId);
                }
            }

            // server.setNeighbourHood(result);
        }
    }

    private void generateSRTsForEachNode() {
        // Step 1: Add groups each node will be interested in
        String type = Configuration.getString(PAR_GROUPS_CONFIG);
        if (type.equals("random")) {
            generateRandomGroupsForNodes();
        } else if (type.equals("file")) {
            generateGroupsFromFile();
        } else {
            throw new AssertException("Unknown parameter in " + PAR_GROUPS_CONFIG);
        }

        // Step 2: Add intermediary group forwarders
        //
        // If a node A is in the middle of two nodes that
        // are interested in a group that node A is not interested,
        // node A will need to forward it
        /*
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
                    nodeToGroups.get(server.getId()).add(group);
                }
            }
        }
        */

        //TODO estou a fazer isto da forma a sério segundo pubsub
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
        Node lastNode = currentPath.get(currentPath.size() - 1);
        Set<Node> neighboursExcludingSource = Utils.getNeighboursExcludingSource(lastNode, beforeLast, linkablePid);
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
        System.out.println("Path is : " +
                path.stream().map(Node::getID).map(Object::toString).collect(Collectors.joining("-")));

        for (Node node : path) {
            BackendInterface server = nodeToBackend(node);
            if (!server.belongsToGroup(group)) {
                System.out.println("adding " + server.getId() + " as forwarder for " + group);
                server.setForwarderOfGroup(group);
                nodeToGroups.get(server.getId()).add(group);
            }
        }
    }

    private void generateGroupsFromFile() {
        Map<String, Integer> stringGroupToInteger = new HashMap<>();
        int groupCounter = 0;
        try {
            List<String> file = Files.readAllLines(Paths.get("example/other/nodes-to-groups.txt"));

            if (file.size() != Network.size() + 1) {
                throw new AssertException("The file and network have different node amounts!" +
                        " File has " + (file.size() - 1) + " nodes, network has " + Network.size() + " nodes.");
            }

            String firstLine = file.get(0);
            String[] groups = firstLine.split(" ");
            for (String group : groups) {
                stringGroupToInteger.put(group, groupCounter++);
            }

            for (int lineIdx = 1; lineIdx < file.size(); lineIdx++) {
                String[] lineContent = file.get(lineIdx).split(" ");
                int nodeIdx = Integer.valueOf(lineContent[0]);
                BackendInterface server = nodeToBackend(Network.get(nodeIdx));

                for (int groupIdx = 1; groupIdx < lineContent.length; groupIdx++) {
                    int group = stringGroupToInteger.get(lineContent[groupIdx]);
                    server.addGroup(group);

                    groupsToMembers.computeIfAbsent(group, k -> new HashSet<>())
                            .add(server.getId());
                    nodeToGroups.computeIfAbsent(server.getId(), k -> new HashSet<>())
                            .add(group);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
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
        Set<Node> nextNeighbours = Utils.getNeighboursExcludingSource(lastNode, nodeBeforeLastNode, linkablePid);

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

    private BackendInterface nodeToBackend(Node node) {
        return (BackendInterface) node.getProtocol(backendPid);
    }
}
