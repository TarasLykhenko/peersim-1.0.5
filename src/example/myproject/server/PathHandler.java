package example.myproject.server;

import example.myproject.Initialization;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.MetadataEntry;
import example.myproject.datatypes.NodePath;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PathHandler {

    private Set<NodePath> paths = new HashSet<>();

    /**
     * Maps the path each node belongs to.
     * Note: If several nodes are on the same path, they will have the same list result.
     */
    private Map<Node, NodePath> nodesToCorrespondingPath = new HashMap<>();

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
                long pathId = entry.getPathId();
                int value = entry.getValue();
                int currentValue = pathsMessagesReceived.get(pathId);

                if (value != currentValue + 1) {
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
            // Falta ver cena de subpath, só deve poder haver 1 em princpio
            List<MetadataEntry> subPath = getVectorSubpath(message.getMetadata(), path);

            Long pathId = getPathIdFromPathNodes(path);
            Integer msgsSentToPath = pathMessagesSent.get(pathId);
            pathMessagesSent.put(pathId, msgsSentToPath + 1);
            subPath.add(new MetadataEntry(pathId, msgsSentToPath + 1));

            // Append new metadata to previous corresponding metadata
            // Need to append correct metadata to correct vector
            metadataToAdd.add(subPath);
        }

        return new Message(message, metadataToAdd, forwarder, destination);
    }

    //TODO ver se isto está bem feito
    private List<MetadataEntry> getVectorSubpath(List<List<MetadataEntry>> metadata,
                                                 NodePath path) {
        for (List<MetadataEntry> vector : metadata) {
            MetadataEntry lastEntry = vector.get(vector.size() - 1);
            if (pathIsSubPath(lastEntry.getPathId(), path)) {
                //>>System.out.println("TRUE!");
                return vector;
            }
        }
        //>>System.out.println("FALSE!");
        return new ArrayList<>();
    }

    boolean pathIsSubPath(long pathId, NodePath path) {
        NodePath subPath = getPathNodesFromPathId(pathId);
        return pathIsSubPath(subPath, path);
    }

    boolean pathIsSubPath(NodePath subPath, NodePath path) {
        Node lastNodeSubPath = subPath.path.get(subPath.path.size() - 1);
        return path.fullPathSet.contains(lastNodeSubPath);
    }

    Set<NodePath> removeForwarderPaths(Message message, Set<NodePath> differentPathsOfInterestNodes) {
        long forwarder = message.getForwarder();
        Iterator<NodePath> it = differentPathsOfInterestNodes.iterator();
        while (it.hasNext()) {
            NodePath nodePath = it.next();
            Node forwarderNode = Network.get(Math.toIntExact(forwarder));
            if (nodePath.pathSetWithoutStart.contains(forwarderNode)) {
                System.out.println("REMOVING! " + nodePath.getPathString());
                it.remove();
            }
        }
        return differentPathsOfInterestNodes;
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
