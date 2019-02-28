package example.myproject;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.MessageBuffer;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

//TODO NOTA: Não sei até que ponto usar um mapa é uma boa ideia.
//TODO provavelmente mudar as datastructures de "Node" para "Long"
public abstract class CausalNeighbourBackend implements BackendInterface {

    /**
     * The nodeID of the node this specific protocol instance belongs to.
     */
    private Long id;

    private Set<List<Node>> paths;

    /**
     * Maps the path each node belongs to.
     * Note: If several nodes are on the same path, they will have the same list result.
     */
    private Map<Node, List<Node>> nodesToCorrespondingPath = new HashMap<>();

    /**
     * Each node belongs to a list of groups
     */
    private Set<Integer> groups = new HashSet<>();

    /**
     * Each node may not belong to a group but must forward said group message
     */
    private Set<Integer> forwardingGroups = new HashSet<>();

    /**
     * Subscription routing tables of the neighbours
     */
    private Map<Long, Set<Integer>> neighbourSRTs = new HashMap<>();

    /**
     * This is a reverse neighboursSRTS - Useful for fast message forwarding.
     */
    private Map<Integer, Set<Long>> groupsToNeighbours = new HashMap<>();


    /**
     *  Each node tracks the number of messages it has sent to each path
     */
    private Map<Long, Integer> pathMessagesSent = new HashMap<>();



    /**
     * Each node tracks the number of messages it has received from the neighbourhood
     */
    private Map<Long, Integer> pathsMessagesReceived = new HashMap<>();


    /**
     * Every time a node publishes a new message, it increments the number
     * of sent messages
     */
    private Integer messagesSent = 0;

    /**
     *  This map is used to store the number of messages received
     *  from each node in the entire system.
     *
     *  It is used to test causality.
     *
     *  When a message is published by a node, the node sends as "data" the
     *  number of messages it has seen from other nodes. When a target node
     *  receives that message, it compares with the number of messages it has
     *  received from those nodes. If any of the entries of this node is smaller
     *  and the message is delivered then causality has been broken.
     */
    private Map<Long, Integer> publisherMessages = new HashMap<>();


    /**
     * When a message is received out of order, it needs to be buffered.
     * These messages are buffered here.
     */
    private MessageBuffer messageBuffer = new MessageBuffer();



    private final int restTime;
    private final int restTimeInterval;

    private long nextAction = 0;


    public CausalNeighbourBackend(String prefix) {
        restTime = Configuration.getInt("rest_time");
        restTimeInterval = Configuration.getInt("rest_time_interval");
    }

    // ----------
    // PUBLIC API
    // ----------

    Message publishMessage() {
        if (shouldPublishMessage()) {
            return generateNewMessage();
        } else {
            return null;
        }
    }

    private Message generateNewMessage() {
        List<Integer> listGroups = new ArrayList<>(groups);
        int listIdx = CommonState.r.nextInt(listGroups.size());
        int groupEntry = listGroups.get(listIdx);

        return new Message(groupEntry, id, id, publisherMessages);
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
    void receiveMessage(Message message) {
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
                bufferMessage(message);
                return;
            } else if (metadataEntry <= currentNodeEntry) {
                // Scenario 3
                handleDuplicateMessage(message);
                return;
            }
        }

        // Scenario 1
        deliverMessage(message);
        //TODO actualizar antes ou depois a metadata?
        forwardMessage(message);
    }

    private void handleDuplicateMessage(Message message) {
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

    // TODO
    void appendMetadataToMessage(Message messageToSend, List<Node> path) {
    }

    // TODO
    abstract Message forwardMessage(Message message);

    // --------------
    // HELPER METHODS
    // --------------

    private boolean shouldPublishMessage() {
        if (CommonState.getTime() > nextAction) {
            int interval = CommonState.r.nextInt(restTimeInterval * 2) - restTimeInterval;
            nextAction = CommonState.getTime() + restTime + interval;
            return true;
        } else {
            return false;
        }
    }

    // TODO
    private void bufferMessage(Message message) {
        messageBuffer.bufferMessage(message);
    }

    // TODO
    private void deliverMessage(Message message) {
        // update entries();
        Map<Long, Integer> metadata = message.getMetadata();

        for (Long nodeId : metadata.keySet()) {
            Integer metadataValue = metadata.get(nodeId);
            pathsMessagesReceived.put(nodeId, metadataValue);
        }

        checkCausality(message);
        // messageBuffer.checkBufferedMessages(); TODO
    }

    private void checkCausality(Message message) {
        Map<Long, Integer> messageCausality = message.getData();

        for (Map.Entry<Long, Integer> entry : messageCausality.entrySet()) {
            Long causalNode = entry.getKey();
            Integer causalNodeValue = entry.getValue();

            Integer currentNodeEntry = publisherMessages
                    .getOrDefault(causalNode, 0);

            // TODO provavelmente
            if (currentNodeEntry  > causalNodeValue) {
                throw new AssertException("Causality has been broken.");
            }
        }
    }

    /**
     * Given a node's neighbourhood, this method returns all nodes that are
     * interested in a message.
     *
     * Note: The result is unordered.
     * @param message
     * @return
     */
    Set<Long> getInterestedNodes(Message message) {
        int group = message.getGroup();

        return groupsToNeighbours.get(group);
    }

    // TODO
    private void checkBufferedMessages(Message message) {
    }


    @Override
    public Set<List<Node>> getNeighbourhood() {
        return paths;
    }

    @Override
    public void setNeighbourHood(Set<List<Node>> differentPaths) {
        this.paths = differentPaths;

        for (List<Node> path : differentPaths) {
            for (Node node : path) {
                nodesToCorrespondingPath.put(node, path);
            }
        }
    }

    @Override
    public Set<Integer> getCopyOfSRT() {
        return new HashSet<>(groups);
    }

    @Override
    public void addNeighbourSRT(Long neighbourId, Set<Integer> srt) {
        this.neighbourSRTs.put(neighbourId, srt);

        for (Integer group : srt) {
            groupsToNeighbours.computeIfAbsent(group, k -> new HashSet<>())
                    .add(neighbourId);
        }
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void addGroup(Integer group) {
        this.groups.add(group);
    }

    @Override
    public boolean belongsToGroup(Integer group) {
        return this.groups.contains(group);
    }

    @Override
    public void setForwarderOfGroup(Integer group) {
        if (this.groups.contains(group)) {
            throw new AssertException("Forwarding a group that the node belongs to?");
        }

        this.forwardingGroups.add(group);
    }

    List<Node> getFullPathOfNode(Node node) {
        return nodesToCorrespondingPath.get(node);
    }
}