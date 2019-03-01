package example.myproject.server;

import example.myproject.datatypes.Message;
import example.myproject.datatypes.MessageBuffer;
import peersim.core.Node;

import java.util.List;
import java.util.Set;

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

        Set<Long> interestedNodes = getInterestedNodes(newMessage);
        Set<Long> liveInterestedNodes = getLiveInterestedNodes(interestedNodes);
        Set<List<Node>> differentPathsOfInterestNodes = getDistinctPaths(liveInterestedNodes);

        for (List<Node> path : differentPathsOfInterestNodes) {
            Message messageToSend = new Message(newMessage);
            appendMetadataToMessage(messageToSend, path);
            sendMessage(node, path.get(0), messageToSend, protocolID);
        }

        return freshMessage;
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