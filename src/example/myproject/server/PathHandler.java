package example.myproject.server;

import example.myproject.Initialization;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.MetadataEntry;
import example.myproject.datatypes.NodePath;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class PathHandler {

    private final long id;
    private Set<NodePath> paths = new HashSet<>();

    /**
     * Maps the path each node belongs to.
     * Note: If several nodes are on the same path, they will have the same list result.
     */
    private Map<Node, NodePath> nodesToCorrespondingPath = new HashMap<>();

    /**
     * Only includes the nodes that are on the path from source to target
     */
    private Map<Node, NodePath> shortestPathToNode = new HashMap<>();

    /**
     * Returns the distance of a node to the current node
     */
    private Map<Node, Integer> nodeToDistance = new HashMap<>();

    /**
     * Each node tracks the number of messages it has sent to each path
     */
    private Map<Long, Integer> pathMessagesSent = new HashMap<>();

    /**
     * Each node tracks the number of messages it has received from the neighbourhood
     */
    private Map<Long, Integer> pathsMessagesReceived = new HashMap<>();

    private Map<Long, NodePath> pathIdsToPathNodes = new HashMap<>();
    private Map<NodePath, Long> pathNodesToPathIds = new HashMap<>();

    public PathHandler(long id) {
        this.id = id;
    }

    public boolean canDeliver(Message message) {
        return false;
    }

    /**
     * This method simply updates the vector's
     *
     * @param message
     * @return
     */
    public Message processMessage(Message message) {
        for (List<MetadataEntry> vector : message.getMetadata()) {
            for (MetadataEntry entry : vector) {
                if (entry.getState() == MetadataEntry.State.JUMP) {
                    continue;
                }

                long pathId = entry.getPathId();
                int value = entry.getValue();
                int currentValue = pathsMessagesReceived.get(pathId);

                if (value > currentValue + 1) {
                    throw new AssertException("This should never happen.");
                }

                pathsMessagesReceived.put(pathId, value);
            }
        }

        return message;
    }

    //TODO isto não me parece que está correcto a actualizar as entradas outdated
    void handleDuplicateMessage(Message message) {
        for (List<MetadataEntry> metadataList : message.getMetadata()) {
            for (MetadataEntry metadataEntry : metadataList) {
                if (metadataEntry.getState() == MetadataEntry.State.JUMP) {
                    continue;
                }

                long pathId = metadataEntry.getPathId();
                int metadataPathValue = metadataEntry.getValue();
                int currentPathValue = pathsMessagesReceived.get(pathId);

                if (metadataPathValue == currentPathValue + 1) {
                    pathsMessagesReceived.put(pathId, metadataPathValue);
                } else if (metadataPathValue > currentPathValue + 1) {
                    throw new AssertException("Interesting scenario 🤔");
                }
            }
        }
    }

    void setNeighbourhood(Set<NodePath> differentPaths) {
        this.paths = differentPaths;

        for (NodePath path : differentPaths) {
            for (Node node : path.path) {
                nodesToCorrespondingPath.put(node, path);
            }
        }
    }

    void setNeighbourhoodAndPathId(NodePath path, long pathId) {
        this.paths.add(path);

        shortestPathToNode.put(path.getLastNodeOnPath(), path);

        for (Node node : path.path) {
            nodesToCorrespondingPath.put(node, path);
        }

        addPathIdMapping(path, pathId);
    }

    void addPathIdMapping(NodePath path, long pathId) {
        pathIdsToPathNodes.put(pathId, path);
        pathNodesToPathIds.put(path, pathId);
        pathMessagesSent.put(pathId, 0);
        pathsMessagesReceived.put(pathId, 0);
    }

    /**
     * When sending a message to a given path, that message may carry several vectors
     * of pathMetadata.
     *
     * For each vector, we need to extract the previous vectors from the same subpath
     * and append the new metadata entry to the corresponding vector
     * @param message
     * @param pathsGroup
     * @return
     */
    Message generateNewMessageForPath(Message message, List<NodePath> pathsGroup,
                                      long forwarder, long destination) {

        List<List<MetadataEntry>> metadataToAdd = new ArrayList<>();

        for (NodePath path : pathsGroup) {

            // Get previous metadata from message to use as base
            // Can be several different vectors
            List<MetadataEntry> subPathCopy = new ArrayList<>(getVectorSubpath(message.getMetadata(), path));
            Long pathId = getPathIdFromPathNodes(path);
            Integer msgsSentToPath = pathMessagesSent.get(pathId);
            pathMessagesSent.put(pathId, msgsSentToPath + 1);
            subPathCopy.add(new MetadataEntry(pathId, msgsSentToPath + 1));

            // Append new metadata to previous corresponding metadata
            // Need to append correct metadata to correct vector
            metadataToAdd.add(subPathCopy);
        }

        addJumpsIfExist(metadataToAdd, destination);

        return new Message(message, metadataToAdd, forwarder, destination);
    }

    Message syncNewMessageForPath(Message message, List<NodePath> pathsGroup, long forwarder, long destination) {
        List<List<MetadataEntry>> metadataToAdd = new ArrayList<>();

        for (NodePath path : pathsGroup) {
            List<MetadataEntry> subPathCopy =
                    new ArrayList<>(getVectorSubpath(message.getMetadata(), path));

            metadataToAdd.add(subPathCopy);
        }

        addJumpsIfExist(metadataToAdd, destination);

        return new Message(message, metadataToAdd, forwarder, destination);
    }

    private void addJumpsIfExist(List<List<MetadataEntry>> metadataToAdd, long destination) {
        NodePath nodePath = shortestPathToNode.get(Network.get((int) destination));
        for (int i = 1; i < nodePath.path.size(); i++) {
            long nodeId = nodePath.path.get(i).getID();
            if (nodeId != destination) {
                for (List<MetadataEntry> vector : metadataToAdd) {
                    vector.add(new MetadataEntry());
                }
            } else {
                break;
            }
        }
    }

    //TODO ver se isto está bem feito
    private List<MetadataEntry> getVectorSubpath(List<List<MetadataEntry>> metadata,
                                                 NodePath path) {
        for (List<MetadataEntry> vector : metadata) {
            MetadataEntry lastEntry = getLastNonNullEntry(vector);
            if (pathIsSubPath(lastEntry.getPathId(), path)) {
                //>> System.out.println("TRUE!");
                return vector;
            } else {
                //>> System.out.println("No");
            }
        }
        //>>System.out.println("FALSE!");
        return new ArrayList<>();
    }

    private MetadataEntry getLastNonNullEntry(List<MetadataEntry> vector) {
        //>> vector.get(vector.size() - 1);
        //>> System.out.println("test!");
        //>> System.out.println(vector);
        for (int i = 1; i <= vector.size(); i++) {
            MetadataEntry entry = vector.get(vector.size() - i);
            //>> System.out.println(entry);
            if (entry.getState() != MetadataEntry.State.JUMP) {
                //>> System.out.println("Returning entry " + entry);
                return entry;
            }
        }
        System.out.println("Returning null");
        return null;
    }

    boolean pathIsSubPath(long pathId, NodePath path) {
        NodePath subPath = getPathNodesFromPathId(pathId);
        return pathIsSubPath(subPath, path);
    }

    boolean pathIsSubPath(NodePath subPath, NodePath path) {
        Node lastNodeSubPath = subPath.path.get(subPath.path.size() - 1);

        //>> System.out.println("Checking if is subpath:");
        //>> subPath.printLn("subPath ");
        //>> path.printLn("full path ");

        return path.fullPathSet.contains(lastNodeSubPath);
    }


    Set<NodePath> removeForwarderPaths(Message message, Set<NodePath> differentPathsOfInterestNodes) {
        long forwarder = message.getForwarder();
        NodePath pathToForwarder = getShortestPathOfNode(Network.get((int) forwarder));
        Set<Node> nodesOnForwarderPath = pathToForwarder.pathSetWithoutStart;

        Iterator<NodePath> it = differentPathsOfInterestNodes.iterator();
        while (it.hasNext()) {
            Set<Node> nodesPath = it.next().pathSetWithoutStart;
            if (!Collections.disjoint(nodesOnForwarderPath, nodesPath)) {
                it.remove();
            }
        }
        return differentPathsOfInterestNodes;
    }

    public NodePath getShortestPathOfNode(Node node) {
        return shortestPathToNode.get(node);
    }

    public Set<Long> getNodesBeyondTargetNode(Set<Long> interestedNodes, long targetNode) {
        //>> System.out.println("START GET BEYOND (Am " + id + ", target is " + targetNode);
        //>> System.out.println("Interested nodes: " + interestedNodes);
        Set<Node> nodesToTarget = shortestPathToNode.get(Network.get((int) targetNode)).fullPathSet;
        Set<Long> result = new HashSet<>();

        //>> shortestPathToNode.get(Network.get((int) targetNode)).printLn("Path of target:  ");
        for (Long nodeId : interestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));
            //>> shortestPathToNode.get(node).printLn("Full path of possible beyond: ");
            if (shortestPathToNode.get(node).fullPathSet.containsAll(nodesToTarget)) {
             //>>   System.out.println(nodeId + " is beyond target " + targetNode + "!");
                result.add(nodeId);
            }
        }

        //>> System.out.println("BEYOND result: " + result);
        return result;
    }

    enum Scenario {
        ONE,
        TWO,
        THREE
    }

    Long getPathIdFromPathNodes(NodePath pathNodes) {
        return pathNodesToPathIds.get(pathNodes);
    }

    NodePath getPathNodesFromPathId(Long pathId) {
        return pathIdsToPathNodes.get(pathId);
    }

    /**
     * A message can have a list of vectors.
     *
     * All vectors' entries should be either have a value higher than the stored path's value
     * or can have lower (duplicate). If any msg entry is higher by 2, then the synch
     * step had a problem that should have not happened.
     *
     * List of scenarios:
     * 1) If all metadata entries are higher by one entry when compared to
     * the receiving node's Data Structure, then the message is ordered;
     *
     * 2) If at least one metadata entry is higher by 2 or more when compared,
     * then at least one message is missing;
     * NOTE: There is now a synchronization step when starting a communication
     * channel with a node, so this scenario should never happen.
     *
     * 3) If at least one metadata entry has a lower value when compared,
     * then the message is a duplicate.
     */
    Scenario evaluationScenario(Message message) {
        //>> System.out.println("Evaluating scenario:");
        for (List<MetadataEntry> listsOfMetadata : message.getMetadata()) {
            for (MetadataEntry entry : listsOfMetadata) {
                if (entry.getState() == MetadataEntry.State.JUMP) {
                    continue;
                }

                long pathId = entry.getPathId();
                int metadataPathValue = entry.getValue();
                int currentNodeEntry = pathsMessagesReceived.get(pathId);

             //>>   getPathNodesFromPathId(pathId).printLn("Path: ");
            //>>    System.out.println(">> msg entry: " + metadataPathValue);
            //>>    System.out.println(">> own entry: " + currentNodeEntry);

                if (metadataPathValue == currentNodeEntry + 1) {
                    // Scenario 1
                    continue;
                } else if (metadataPathValue > currentNodeEntry + 1) {
                    // Scenario 2
                    // bufferMessage(message);
                    getPathNodesFromPathId(pathId).printLn("Path ");
                    System.out.println("Metadata value: " + metadataPathValue);
                    System.out.println("Own value: " + currentNodeEntry);
                    throw new AssertException("Because of the new connection synch, this should never happen.");
                    // return Scenario.TWO;
                } else if (metadataPathValue <= currentNodeEntry) {
                    // Scenario 3
                    // handleDuplicateMessage(message);
                    return Scenario.THREE;
                }
            }
        }
        // Scenario 1
        return Scenario.ONE;
        // deliverMessage(message);
        //TODO actualizar antes ou depois a metadata?
        // forwardMessages(message);
    }

    // ------------------
    // INTERFACE METHODS
    // ------------------

    Set<NodePath> getNeighbourhood() {
        return paths;
    }

    NodePath getFullPathOfNode(Node node) {
        return nodesToCorrespondingPath.get(node);
    }

    String printStatus() {
        StringBuilder builder = new StringBuilder();
        builder.append("PATHS SENT: ").append(System.lineSeparator());

        for (Map.Entry<Long, Integer> entry : pathMessagesSent.entrySet()) {
            long pathId = entry.getKey();
            int pathValue = entry.getValue();
            NodePath nodepath = Initialization.pathsToPathLongs.get(pathId);

            builder.append(nodepath.getPathString() + " : " + pathValue).append(System.lineSeparator());
        }

        builder.append("PATHS RECEIVED: ").append(System.lineSeparator());
        for (Map.Entry<Long, Integer> entry : pathsMessagesReceived.entrySet()) {
            long pathId = entry.getKey();
            int pathValue = entry.getValue();
            NodePath nodepath = Initialization.pathsToPathLongs.get(pathId);

            builder.append(nodepath.getPathString() + " : " + pathValue).append(System.lineSeparator());
        }
        return builder.toString();
    }
}
