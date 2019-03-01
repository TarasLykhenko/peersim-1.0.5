package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import peersim.core.Node;

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
     *  Each node tracks the number of messages it has sent to each path
     */
    private Map<Long, Integer> pathMessagesSent = new HashMap<>();

    /**
     * Each node tracks the number of messages it has received from the neighbourhood
     */
    private Map<Long, Integer> pathsMessagesReceived = new HashMap<>();


    public boolean canDeliver(Message message) {
        return false;
    }

    /**
     * This method simply updates the vector's
     * @param messages
     * @return
     */
    public List<Message> processMessages(List<Message> messages) {
        for (Message message : )
        return null;
    }

    void handleDuplicateMessage(Message message) {
        Map<Long, Integer> metadata = message.getMetadata();

        for (Long nodeId : metadata.keySet()) {
            Integer metadataValue = metadata.get(nodeId);
            Integer currentNodeEntry = pathsMessagesReceived.get(nodeId);

            if (metadataValue == currentNodeEntry + 1) {
                pathsMessagesReceived.put(nodeId, metadataValue);
            } else if (metadataValue > currentNodeEntry +1) {
                throw new AssertException("Interesting scenario 🤔");
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

    enum Scenario {
        ONE,
        TWO,
        THREE
    }

    // TODO
    void appendMetadataToMessage(Message messageToSend, List<Node> path) {
    }

    /**
     * A message can have three different states:
     *  1) If all metadata entries are higher by one entry when compared to
     *  the receiving node's Data Structure, then the message is ordered;
     *
     *  2) If at least one metadata entry is higher by 2 or more when compared,
     *  then at least one message is missing;
     *
     *  3) If at least one metadata entry has a lower value when compared,
     *  then the message is a duplicate.
     *
     * @param message
     */
    Scenario evaluationScenario(Message message) {
        for (Map.Entry<Long, Integer> entry : message.getMetadata().entrySet()) {
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
                return Scenario.TWO;
            } else if (metadataEntry <= currentNodeEntry) {
                // Scenario 3
                // handleDuplicateMessage(message);
                return Scenario.THREE;
            }
        }

        // Scenario 1
        return Scenario.ONE;
        // deliverMessage(message);
        //TODO actualizar antes ou depois a metadata?
        // forwardMessage(message);
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
