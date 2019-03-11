package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    Set<Node> activeConnections = new HashSet<>();
    Set<Node> downConnections = new HashSet<>();


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

        return prepareMessageToForward(freshMessage);
    }

    private List<Message> prepareMessageToForward(Message message) {
        Set<Long> interestedNodes = getInterestedNodes(message);
        Set<Long> liveInterestedNodes = getLiveInterestedNodes(interestedNodes);
        Set<List<Node>> differentPathsOfInterestNodes = getDistinctPaths(liveInterestedNodes);
        Collection<List<List<Node>>> groupedDistinctPaths = groupDistinctPaths(differentPathsOfInterestNodes);

        List<Message> differentMessagesToSend = new ArrayList<>();
        for (List<List<Node>> pathsGroup : groupedDistinctPaths) {
            Message messageToSend = pathHandler.generateNewMessageForPath(message, pathsGroup);
            // pathHandler.appendMetadataToMessage(messageToSend, path);
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

        printPathLongs("Set of nodes", liveInterestedNodes);

        for (Long nodeId : liveInterestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));
            List<Node> fullPathOfNode = getFullPathOfNode(node);
            distinctPaths.add(fullPathOfNode);
        }


        for (List<Node> path : distinctPaths) {
            printPath("Distinct path", path);
        }
        return distinctPaths;
    }


    /**
     * Given a list of distinct paths, groups them according to different subpaths
     * Groups the paths also by seeing which active connection is alive.
     *
     * Example (for lvl 1): We split according to similar nodes on the 2nd index
     *  Subpaths:   2 0 1
     *              2 5 11
     *              2 5 12
     *              2 6 13
     *              2 6 14
     *
     *  The groupings are [2 0 1], [{2 5 11}, {2 5 12}] and [{2 6 13}, {2 6 14}]
     *  The messages will be sent to nodes 0, 5 and 6.
     *
     *  Node 0 will receive metadata for [2 0 1]
     *  Node 5 will receive metadata for [2 5 11, 2 5 12]
     *  Node 6 will receive metadata for [2 6 13, 2 6 14]
     * @param distinctPaths
     * @return
     */
    private Collection<List<List<Node>>> groupDistinctPaths(Set<List<Node>> distinctPaths) {
        Map<Long, List<List<Node>>> result = new HashMap<>();
        int counter = 0; // Tracks how many lists were added

        for (List<Node> path : distinctPaths) {
            int lvl = 1;
            while (lvl < path.size()) {
                Node node = path.get(lvl);
                if (downConnections.contains(node)) {
                    lvl++;
                } else {
                    long entry = node.getID();
                    result.computeIfAbsent(entry, k -> new ArrayList<>()).add(path);
                    break;
                }
            }
            if (lvl == path.size()) {
                throw new AssertException("This is an interesting case");
            }
        }

        return result.values();
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
     * @param messages The list of messages that are to be sent.
     *                IMPORTANT: They should already be processed
     */
    abstract void forwardMessages(List<Message> messages);


    // --------------------------------------------------------
    // ---------------- MESSAGE RECEIVE PROCESS ---------------
    // --------------------------------------------------------

    void receiveMessage(Message message) {
        PathHandler.Scenario scenario = pathHandler.evaluationScenario(message);
        switch (scenario) {
            case ONE:
                Message causallyApprovedMessage = causalityHandler.processMessage(message);
                Message processedMessage = pathHandler.processMessage(causallyApprovedMessage);
                List<Message> messagesToForward = prepareMessageToForward(processedMessage);
                forwardMessages(messagesToForward);
                break;

            case TWO:
                System.exit(1);
                // messageBuffer.bufferMessage(message);
                break;

            case THREE:
                // TODO provavelmente tem de propagar as metadatas para o resto
                pathHandler.handleDuplicateMessage(message);
                break;
        }
    }

    // --------------
    // HELPER METHODS
    // --------------

    public static void printPath(String msg, Collection<Node> path) {
        String result = path.stream()
                .map(Node::getID)
                .map(Object::toString)
                .collect(Collectors.joining(":"));
        System.out.println(msg + " - " + result);
    }

    public static void printPathLongs(String msg, Collection<Long> path) {
        String result = path.stream()
                .map(Object::toString)
                .collect(Collectors.joining(":"));
        System.out.println(msg + " - " + result);
    }

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