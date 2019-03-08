package example.myproject.server;

import example.myproject.datatypes.Message;
import example.myproject.datatypes.MessageBuffer;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

//TODO NOTA: Não sei até que ponto usar um mapa é uma boa ideia.
//TODO provavelmente mudar as datastructures de "Node" para "Long"
public abstract class CausalNeighbourBackend implements BackendInterface {

    /**
     * The nodeID of the node this specific protocol instance belongs to.
     */
    private Long id;

    private MessagePublisher messagePublisher;
    private PathHandler pathHandler;
    private CausalityHandler causalityHandler;
    /**
     * When a message is received out of order, it needs to be buffered.
     * These messages are buffered here.
     */
    private MessageBuffer messageBuffer = new MessageBuffer();


    public CausalNeighbourBackend(String prefix) {

    }

    // ----------
    // PUBLIC API
    // ----------

    /**
     * Publishes a new message. This message may be cloned to be sent to different nodes.
     * @return
     */
    List<Message> publishMessage() {
        Message freshMessage = messagePublisher.publishMessage();

        if (freshMessage == null) {
            return null;
        }

        causalityHandler.addPublisherState(freshMessage);

        Set<Long> interestedNodes = getInterestedNodes(freshMessage);
        Set<Long> liveInterestedNodes = getLiveInterestedNodes(interestedNodes);
        Set<List<Node>> differentPathsOfInterestNodes = getDistinctPaths(liveInterestedNodes);

        List<Message> differentMessagesToSend = new ArrayList<>();
        for (List<Node> path : differentPathsOfInterestNodes) {
            Message messageToSend = new Message(freshMessage);
            messageToSend.setNextDestination(path.get(0).getID());
            pathHandler.appendMetadataToMessage(messageToSend, path);
            differentMessagesToSend.add(messageToSend);
        }

        return differentMessagesToSend;
    }



    /**
     * Given a set of nodes, returns the different paths that englobe all nodes.
     * Meaning if there are only two nodes and both are on the same path, only one
     * path is returned.
     *
     * @return
     */
    private Set<List<Node>> getDistinctPaths(Set<Long> liveInterestedNodes) {
        Set<List<Node>> distinctPaths = new HashSet<>();

        String collect =
                liveInterestedNodes.stream().map(Objects::toString).collect(Collectors.joining(" "));
        System.out.println("Set of nodes: " + collect);

        for (Long nodeId : liveInterestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));
            List<Node> fullPathOfNode = getFullPathOfNode(node);
            distinctPaths.add(fullPathOfNode);
        }

        for (List<Node> path : distinctPaths) {
            String pathCollect =
                    path.stream()
                            .map(Node::getID)
                            .map(Objects::toString)
                            .collect(Collectors.joining(" "));
            System.out.println("Distinct path: " + pathCollect);
        }
        return distinctPaths;
    }

    // TODO Isto parece-me batota? Ver directamente se o nó está vivo, não sei
    // se posso fazer isto

    /**
     * Given a set of nodes, this method returns a new set containing
     * only the nodes that are currently alive.
     * @param interestedNodes
     * @return
     */
    private Set<Long> getLiveInterestedNodes(Set<Long> interestedNodes) {
        Set<Long> result = new HashSet<>();

        for (Long nodeId : interestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));

            if (node.isUp()) {
                result.add(nodeId);
            }
        }

        return result;
    }

















    /**
     * The frontend receives a list of messages ready to be forwarded and directly
     * forwards the messages, without doing any processing
     *
     * @param message The list of messages that are to be sent.
     *                IMPORTANT: They should already be processed
     */
    abstract void forwardMessages(List<Message> message);


    // --------------------------------------------------------
    // ---------------- MESSAGE RECEIVE PROCESS ---------------
    // --------------------------------------------------------

    void receiveMessage(Message message) {
        PathHandler.Scenario scenario = pathHandler.evaluationScenario(message);
        switch (scenario) {
            case ONE:
                List<Message> messages = messageBuffer.processMessages(message);
                List<Message> causallyApprovedMessages = causalityHandler.processMessages(messages);
                List<Message> processedMessages = pathHandler.processMessages(causallyApprovedMessages);
                List<Message> messagesToForward = messagePublisher.forwardMessages(processedMessages);
                forwardMessages(messagesToForward);
                break;

            case TWO:
                messageBuffer.bufferMessage(message);
                break;

            case THREE:
                pathHandler.handleDuplicateMessage(message);
                break;
        }
    }

    // --------------
    // HELPER METHODS
    // --------------

    List<Node> getFullPathOfNode(Node node) {
        return pathHandler.getFullPathOfNode(node);
    }

    @Override
    public void init(Long id) {
        this.id = id;
        this.messagePublisher = new MessagePublisher(id);
        this.pathHandler = new PathHandler();
        this.causalityHandler = new CausalityHandler();
    }

    /**
     * Given a node's neighbourhood, this method returns all nodes that are
     * interested in a message.
     *
     * Note: The result is unordered.
     *
     * @param message
     * @return
     */
    private Set<Long> getInterestedNodes(Message message) {
        return messagePublisher.getInterestedNodes(message);
    }

    @Override
    public Set<List<Node>> getNeighbourhood() {
        return pathHandler.getNeighbourhood();
    }

    @Override
    public void setNeighbourHood(Set<List<Node>> differentPaths) {
        pathHandler.setNeighbourhood(differentPaths);
    }

    @Override
    public Set<Integer> getCopyOfSRT() {
        return messagePublisher.getCopyOfSRT();
    }

    @Override
    public void addNeighbourSRT(Long neighbourId, Set<Integer> srt) {
        messagePublisher.addNeighbourSRT(neighbourId, srt);
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void addGroup(Integer group) {
        messagePublisher.addGroup(group);
    }

    @Override
    public boolean belongsToGroup(Integer group) {
        return messagePublisher.belongsToGroup(group);
    }

    @Override
    public void setForwarderOfGroup(Integer group) {
        messagePublisher.setForwarderOfGroup(group);
    }
}