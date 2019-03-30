package example.myproject.server;

import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.NodePath;
import example.myproject.datatypes.PathMessage;
import javafx.util.Pair;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    protected long id;

    private MessagePublisher messagePublisher;
    private PathHandler pathHandler;
    private CausalityHandler causalityHandler;
    private ConnectionHandler connectionHandler;

    public CausalNeighbourBackend(String prefix) {
    }

    // ----------
    // PUBLIC API
    // ----------

    /**
     * Publishes a new message.
     *
     * This message may be cloned to be sent to different nodes.
     *
     * @return A list of messages to be forwarded to other servers.
     */
    List<Message> publishMessage() {
        Message freshMessage = messagePublisher.publishMessage();

        return publishMessageImpl(freshMessage);
    }

    List<Message> publishMessage(int topic) {
        Message freshMessage = messagePublisher.publishMessage(topic);

        return publishMessageImpl(freshMessage);
    }

    private List<Message> publishMessageImpl(Message freshMessage) {
        if (freshMessage == null) {
            return Collections.emptyList();
        }

        causalityHandler.addPublisherState(freshMessage);

        System.out.println("Publishing a message!");
        freshMessage.printMessage();

        List<Pair<Message, Message>> messages = prepareMessageToForward(freshMessage);
        return connectionHandler.handleOutgoingMessages(messages);
    }

    /**
     * Receives a message.
     *
     * This message may then be forwarder to other nodes.
     *
     * @param message The message that this server received
     * @return A list of messages to be forwarded to other servers.
     */
    List<Message> receiveMessage(Message message) {
        PathHandler.Scenario scenario = pathHandler.evaluationScenario(message);
        List<Pair<Message, Message>> resultList = null;

        switch (scenario) {
            case NORMAL:
                connectionHandler.handleIngoingMessage(message);
                long startTimeProcess = System.currentTimeMillis();
                causalityHandler.processMessage(message);
                /*
                if (messagePublisher.belongsToGroup(message.getGroup())) {
                    causalityHandler.processMessage(message);
                }
                */
                pathHandler.processMessage(message);
                resultList = prepareMessageToForward(message);
                long endTimeProcess = System.currentTimeMillis();

                System.out.println("Time to process msg: " + (endTimeProcess - startTimeProcess));
                break;

            case MISSING:
                throw new AssertException("This scenario is impossible.");
                // messageBuffer.bufferMessage(message);
                // break;

            case DUPLICATE:
                // TODO provavelmente tem de propagar as metadatas para o resto
                pathHandler.handleDuplicateMessage(message);
                Message msg = connectionHandler.checkMessagesWithTooManyJumps(message);
                resultList = prepareMessageToForward(msg);


                //causalityHandler.handleDuplicateMessage(message); //TODO nao sei ate que ponto isto é necessario
                // throw new AssertException("TODO must implement this scenario! : " + id);
                break;
        }

        if (Utils.DEBUG_V) {
            System.out.println("Scenario " + scenario);
        }

        return connectionHandler.handleOutgoingMessages(resultList);
    }

    private List<Pair<Message, Message>> prepareMessageToForward(Message message) {
        Set<Long> interestedNodes = getInterestedNodes(message);
        Set<NodePath> differentPathsOfInterestNodes = getDistinctPaths(interestedNodes);
        removeForwarderPaths(message, differentPathsOfInterestNodes);
        Collection<List<NodePath>> groupedDistinctPaths = groupDistinctPaths(differentPathsOfInterestNodes);

        List<Pair<Message, Message>> differentMessagesToSend = new ArrayList<>();
        for (List<NodePath> pathsGroup : groupedDistinctPaths) {
            long destination = getDestination(pathsGroup);
            Pair<Message, Message> messageToSend = pathHandler.generateNewMessageForPath(message, pathsGroup, id, destination);
            differentMessagesToSend.add(messageToSend);
        }

        return differentMessagesToSend;
    }

    Message syncPrepareMessageToForward(Message message, long syncingNode) {
        Set<Long> interestedNodes = getInterestedNodes(message);
        Set<Long> nodesBeyondTarget = getNodesBeyondTargetNode(interestedNodes, syncingNode);
        Set<NodePath> differentPathsOfInterestNodes = getDistinctPaths(nodesBeyondTarget);
        removeForwarderPaths(message, differentPathsOfInterestNodes);
        Collection<List<NodePath>> groupedDistinctPaths = groupDistinctPaths(differentPathsOfInterestNodes);

        if (groupedDistinctPaths.size() != 1) {
            throw new AssertException("This should always be 1");
        }

        List<NodePath> path = groupedDistinctPaths.iterator().next();
        return pathHandler.syncNewMessageForPath(message, path, id, syncingNode);
    }

    private Set<Long> getNodesBeyondTargetNode(Set<Long> interestedNodes, long targetNode) {
        return pathHandler.getNodesBeyondTargetNode(interestedNodes, targetNode);
    }

    private void removeForwarderPaths(Message message,
                                      Set<NodePath> differentPathsOfInterestNodes) {
        pathHandler.removeForwarderPaths(message, differentPathsOfInterestNodes);
    }

    private long getDestination(List<NodePath> pathsGroup) {
        // We can use any nodePath to get destination, as all share
        // the same first activeConnection node
        NodePath nodePath = pathsGroup.get(0);
        for (int i = 1; i < nodePath.path.size(); i++) {
            Node node = nodePath.path.get(i);
            if (connectionHandler.containsConnection(node)) {
                return node.getID();
            }
        }

        //TODO deal with this
        throw new AssertException("Every possible connection is down." + System.lineSeparator() +
                "Active connections: " + Utils.nodesToLongs(connectionHandler.getActiveConnections()) + System.lineSeparator() +
                "Down connections: " + Utils.nodesToLongs(connectionHandler.getDownConnections()));
    }


    /**
     * Given a set of nodes, returns the different paths that englobe all nodes.
     * Meaning if there are only two nodes and both are on the same path, only one
     * path is returned.
     *
     * @return The set of different paths that include all interested nodes
     */
    private Set<NodePath> getDistinctPaths(Set<Long> liveInterestedNodes) {
        Set<NodePath> distinctPaths = new HashSet<>();

        if (Utils.DEBUG_V) {
            NodePath.printPathLongs("Set of nodes", liveInterestedNodes);
        }

        for (Long nodeId : liveInterestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));
            NodePath fullPathOfNode = getShortestPathOfNode(node);
            //>> System.out.println("Checking " + nodeId + " - " + fullPathOfNode);

            boolean setContainsSubpath = false;

            for (NodePath distinctPath : distinctPaths) {
                // First check if an existing path already exists that covers the
                // current node
                if (pathHandler.pathIsSubPath(fullPathOfNode, distinctPath)) {
                    setContainsSubpath = true;
                }

                // Then check if there is an existing path that is covered
                // by the current node
                if (pathHandler.pathIsSubPath(distinctPath, fullPathOfNode)) {
                    //>> System.out.println("Removing previous existing path (" + distinctPath + ", " + fullPathOfNode + ")");
                    distinctPaths.remove(distinctPath);
                    break;
                }
            }

            if (!setContainsSubpath) {
                //>> System.out.println("Adding " + fullPathOfNode.getPathString());
                distinctPaths.add(fullPathOfNode);
            }
        }

        //>> isto
        /*
        for (NodePath path : distinctPaths) {
            path.printLn("Distinct path");
        }
        */


        return distinctPaths;
    }

    void checkForCrashes() {
        this.connectionHandler.checkForCrashes();
    }


    /**
     * Given a list of distinct paths, groups them according to different subpaths
     * Groups the paths also by seeing which active connection is alive.
     *
     * Example (for lvl 1): We split according to similar nodes on the 2nd index
     * Subpaths:   2 0 1
     * 2 5 11
     * 2 5 12
     * 2 6 13
     * 2 6 14
     *
     * The groupings are [2 0 1], [{2 5 11}, {2 5 12}] and [{2 6 13}, {2 6 14}]
     * The messages will be sent to nodes 0, 5 and 6.
     *
     * Node 0 will receive metadata for [2 0 1]
     * Node 5 will receive metadata for [2 5 11, 2 5 12]
     * Node 6 will receive metadata for [2 6 13, 2 6 14]
     *
     * @param distinctPaths A set of distinct paths that may overlap
     * @return A collection of paths grouped by overlaps
     */
    private Collection<List<NodePath>> groupDistinctPaths(Set<NodePath> distinctPaths) {
        Map<Long, List<NodePath>> result = new HashMap<>();

        for (NodePath path : distinctPaths) {
            int lvl = 1;
            while (lvl < path.path.size()) {
                Node node = path.path.get(lvl);
                if (connectionHandler.getDownConnections().contains(node)) {
                    if ( Utils.DEBUG_V) {
                        System.out.println("SAD!");
                    }
                    lvl++;
                } else {
                    long entry = node.getID();
                    result.computeIfAbsent(entry, k -> new ArrayList<>()).add(path);
                    break;
                }
            }
            if (lvl == path.path.size()) {
                throw new AssertException("This is an interesting case");
            }
        }

        return result.values();
    }


    //TODO Por acaso pensando bem, as msgs devem ser sempre feitas para todos os nos,
    // mesmo se tiverem mortos
    /**
     * Given a set of nodes, this method returns a new set containing
     * only the nodes that we currently have connections to
     *
     * @param interestedNodes All nodes that are interested in a given message
     * @return The subset of the nodes that we have an active connection to
     */
    /*
    private Set<Long> getLiveInterestedNodes(Set<Long> interestedNodes) {
        Set<Long> result = new HashSet<>();

        for (Long nodeId : interestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));

            if (getActiveConnections().contains(node)) {
                result.add(nodeId);
            }
        }

        return result;
    }
    */


    // --------------
    // HELPER METHODS
    // --------------

    /**
     * Returns the path from this node to the target node
     *
     * @param otherNode A target node
     * @return The path to the otherNode
     */
    private NodePath getShortestPathOfNode(Node otherNode) {
        return pathHandler.getShortestPathOfNode(otherNode);
    }

    @Override
    public void init(Long id) {
        this.id = id;
        this.messagePublisher = new MessagePublisher(id);
        this.pathHandler = new PathHandler(id);
        this.causalityHandler = new CausalityHandler(id);
        this.connectionHandler = new ConnectionHandler(id, this);
    }

    /**
     * Given a node's neighbourhood, this method returns all nodes that are
     * interested in a message.
     *
     * Note: The result is unordered.
     *
     * @param message A message object
     * @return The nodes in the neighbourhood that are interested in the message
     */
    private Set<Long> getInterestedNodes(Message message) {
        return messagePublisher.getInterestedNodes(message);
    }

    @Override
    public Set<NodePath> getNeighbourhood() {
        return pathHandler.getNeighbourhood();
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
    public Set<Integer> getGroups() {
        return messagePublisher.getGroups();
    }

    @Override
    public void setForwarderOfGroup(Integer group) {
        messagePublisher.setForwarderOfGroup(group);
    }

    @Override
    public String printStatus() {
        String backendStatus = "Node " + id + " status";
        String connectionHandlerStatus = connectionHandler.printStatus();

        String messagePublisherStatus = messagePublisher.printStatus();
        String causalityHandlerStatus = causalityHandler.printStatus();
        String pathHandlerStatus = pathHandler.printStatus();

        String result = backendStatus + System.lineSeparator() +
                connectionHandlerStatus + System.lineSeparator() +
                messagePublisherStatus + System.lineSeparator() +
                causalityHandlerStatus + System.lineSeparator();

        if (Utils.DEBUG_V) {
            result += pathHandlerStatus;
        }

        return result;
    }

    @Override
    public void checkCrashedConnections() {
        connectionHandler.checkCrashedConnections();
    }

    @Override
    public List<Long> handleNewConnection(long sender) {
        return connectionHandler.handleNewConnection(sender);
    }

    @Override
    public List<Message> compareHistory(long sender, List<Long> historyFrom) {
        return connectionHandler.compareHistory(sender, historyFrom);
    }

    @Override
    public void startActiveConnection(long connectionStarterId) {
        Node otherNode = Network.get((int) connectionStarterId);
        connectionHandler.startActiveConnection(otherNode);
    }

    /**
     * Adds the pathMessage's pathId and path to the inner paths or
     * or outer path (depending on the distance travelled)
     * @param pathMessage The message to be spread
     * @return True if the message should be
     */
    boolean processNewReceivedPath(PathMessage pathMessage) {
        NodePath nodePath = pathMessage.getNodePath();
        long pathId = pathMessage.getPathId();

        // Only accept relevant paths inside the inner path
        // (e.g. paths that contain the current node)
        if (senderNodeInsideInnerArea(pathMessage) && !nodePath.containsNode(id)) {
            return false;
        }

        if (senderNodeInsideInnerArea(pathMessage)) {
            pathHandler.addInnerPathIdMapping(nodePath, pathId);
            return true;
        } else if (senderNodeInsideOuterArea(pathMessage)) {
            pathHandler.addOuterPathIdMapping(nodePath, pathId);
            return true;
        }

        return false;
    }

    private boolean senderNodeInsideInnerArea(PathMessage pathMessage) {
        return pathMessage.getCurrentDistance() <= Utils.DELTA + 1;
    }

    private boolean senderNodeInsideOuterArea(PathMessage pathMessage) {
        return pathMessage.getCurrentDistance() <= Utils.DELTA_MAX_SIZE;
    }

    PathHandler getPathHandler() {
        return this.pathHandler;
    }
}