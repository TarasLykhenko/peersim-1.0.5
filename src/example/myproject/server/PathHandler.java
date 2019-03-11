package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.MessageMap;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PathHandler {

    private Set<List<Node>> paths;

    /**
     * Maps the path each node belongs to.
     * Note: If several nodes are on the same path, they will have the same list result.
     */
    private Map<Node, List<Node>> nodesToCorrespondingPath = new HashMap<>();

    /**
     * Each node tracks the number of messages it has sent to each path
     */
    private Map<Long, Integer> pathMessagesSent = new HashMap<>();

    /**
     * Each node tracks the number of messages it has received from the neighbourhood
     */
    private Map<Long, Integer> pathsMessagesReceived = new HashMap<>();

    private Map<Long, List<Node>> pathIdsToPathNodes = new HashMap<>();
    private Map<List<Node>, Long> pathNodesToPathIds = new HashMap<>();


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
        return message;
    }

    void handleDuplicateMessage(Message message) {
        for (Map<Long, Integer> metadataVector : message.getMetadata()) {
            for (Long nodeId : metadataVector.keySet()) {
                Integer metadataValue = metadataVector.get(nodeId);
                Integer currentNodeEntry = pathsMessagesReceived.get(nodeId);

                if (metadataValue == currentNodeEntry + 1) {
                    pathsMessagesReceived.put(nodeId, metadataValue);
                } else if (metadataValue > currentNodeEntry + 1) {
                    throw new AssertException("Interesting scenario 🤔");
                }
            }
        }
    }

    void setNeighbourhood(Set<List<Node>> differentPaths) {
        this.paths = differentPaths;

        for (List<Node> path : differentPaths) {
            for (Node node : path) {
                nodesToCorrespondingPath.put(node, path);
            }
        }
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
    Message generateNewMessageForPath(Message message, List<List<Node>> pathsGroup) {

        System.out.println("Sending a message with the following vectors:");
        for (List<Node> path : pathsGroup) {
            CausalNeighbourBackend.printPath("Vector entry",path);
        }

        List<Map<Long, Integer>> metadataToAdd = new ArrayList<>();
        for (List<Node> path : pathsGroup) {
            // Get previous metadata from message to use as base
            // Can be several different vectors
            // Falta ver cena de subpath, só deve poder haver 1 em princpio
            List<Map<Long, Integer>> metadata = message.getMetadata();

            Long pathId = getPathIdFromPathNodes(path);
            Integer msgsSentToPath = pathMessagesSent.get(pathId);
            pathMessagesSent.put(pathId, msgsSentToPath + 1);
           // metadataToAdd.put(pathId, msgsSentToPath + 1);

            // Append new metadata to previous corresponding metadata
            // Need to append correct metadata to correct vector
        }

        //TODO tirar este hardcoded destination
        return new Message(message, metadataToAdd, 0);
    }

    enum Scenario {
        ONE,
        TWO,
        THREE
    }

    // TODO
    void appendMetadataToMessage(Message messageToSend, List<Node> path) {

    }

    Long getPathIdFromPathNodes(List<Node> pathNodes) {
        return pathNodesToPathIds.get(pathNodes);
    }

    List<Node> getPathNodesFromPathId(Integer pathId) {
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
        for (Map<Long, Integer> listsOfMetadata : message.getMetadata()) {
            for (Map.Entry<Long, Integer> entry : listsOfMetadata.entrySet()) {
                Long nodeId = entry.getKey();
                Integer metadataEntry = entry.getValue();
                // TODO vai dar NPE soon
                Integer currentNodeEntry = pathsMessagesReceived.get(nodeId);

                if (metadataEntry == currentNodeEntry + 1) {
                    // Scenario 1
                    continue;
                } else if (metadataEntry > currentNodeEntry + 1) {
                    // Scenario 2
                    // bufferMessage(message);
                    throw new AssertException("Because of the new connection synch, this should never happen.");
                    // return Scenario.TWO;
                } else if (metadataEntry <= currentNodeEntry) {
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

    //TODO
    public boolean pathIsSubpath(Long pathId, List<Node> nodes) {
        return false;
    }

    // ------------------
    // INTERFACE METHODS
    // ------------------

    Set<List<Node>> getNeighbourhood() {
        return paths;
    }

    List<Node> getFullPathOfNode(Node node) {
        return nodesToCorrespondingPath.get(node);
    }
}
