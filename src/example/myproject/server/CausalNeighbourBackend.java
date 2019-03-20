package example.myproject.server;

import example.myproject.Initialization;
import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.MetadataEntry;
import example.myproject.datatypes.NodePath;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

//TODO NOTA: Não sei até que ponto usar um mapa é uma boa ideia.
//TODO provavelmente mudar as datastructures de "Node" para "Long"
public abstract class CausalNeighbourBackend implements BackendInterface {

    private static final String PAR_LINKABLE = "linkable";

    /**
     * The nodeID of the node this specific protocol instance belongs to.
     */
    protected long id;
    private final int linkablePid;

    private MessagePublisher messagePublisher;
    private PathHandler pathHandler;
    private CausalityHandler causalityHandler;

    Set<Node> activeConnections = new HashSet<>();
    Set<Node> downConnections = new HashSet<>();

    Set<Node> inActivationConnections = new HashSet<>();

    /**
     * Buffer used when starting a connection with a new node.
     * This is used because between starting the connection and synchronizing,
     * messages cannot be sent (otherwise FIFO will be broken)
     */
    Map<Node, List<Message>> nodeMessageQueue = new HashMap<>();

    public CausalNeighbourBackend(String prefix) {
        this.linkablePid = Configuration.getPid(prefix + "." + PAR_LINKABLE);
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
            return Collections.emptyList();
        }

        causalityHandler.addPublisherState(freshMessage);

        System.out.println("Publishing a message!");
        return prepareMessageToForward(freshMessage);
    }

    private List<Message> prepareMessageToForward(Message message) {
        Set<Long> interestedNodes = getInterestedNodes(message);
        return prepareMessageToForward(message, interestedNodes);
    }

    private List<Message> prepareMessageToForward(Message message, Set<Long> interestedNodes) {
        Set<Long> liveInterestedNodes = getLiveInterestedNodes(interestedNodes);
        Set<NodePath> differentPathsOfInterestNodes = getDistinctPaths(liveInterestedNodes);
        removeForwarderPaths(message, differentPathsOfInterestNodes);
        Collection<List<NodePath>> groupedDistinctPaths = groupDistinctPaths(differentPathsOfInterestNodes);

        List<Message> differentMessagesToSend = new ArrayList<>();
        for (List<NodePath> pathsGroup : groupedDistinctPaths) {
            long destination = getDestination(pathsGroup);
            Message messageToSend = pathHandler.generateNewMessageForPath(message, pathsGroup, id, destination);
            addGcInformation(messageToSend, pathsGroup);
            differentMessagesToSend.add(messageToSend);
        }

        addMessagesToConnectionQueues(differentMessagesToSend);

        return differentMessagesToSend;
    }

    private void addMessagesToConnectionQueues(List<Message> differentMessagesToSend) {
        Iterator<Message> iterator = differentMessagesToSend.iterator();
        while (iterator.hasNext()) {
            Message message = iterator.next();
            long destination = message.getNextDestination();
            Node dstNode = Network.get((int) destination);
            if (inActivationConnections.contains(dstNode)) {
                nodeMessageQueue.computeIfAbsent(dstNode, k -> new ArrayList<>()).add(message);
                System.out.println("Removing msg to dst " + destination);
                iterator.remove();
            }
        }
    }


    private Message syncPrepareMessageToForward(Message message, long syncingNode) {
        Set<Long> interestedNodes = getInterestedNodes(message);
        Set<Long> nodesBeyondTarget = getNodesBeyondTargetNode(interestedNodes, syncingNode);
        Set<NodePath> differentPathsOfInterestNodes = getDistinctPaths(nodesBeyondTarget);
        removeForwarderPaths(message, differentPathsOfInterestNodes);
        Collection<List<NodePath>> groupedDistinctPaths = groupDistinctPaths(differentPathsOfInterestNodes);

        if (groupedDistinctPaths.size() != 1) {
            throw new AssertException("This should always be 1");
        }

        for (List<NodePath> pathsGroup : groupedDistinctPaths) {
            return pathHandler.syncNewMessageForPath(message, pathsGroup, id, syncingNode);
        }

        throw new AssertException("boom.");
    }

    private Set<Long> getNodesBeyondTargetNode(Set<Long> interestedNodes, long targetNode) {
        return pathHandler.getNodesBeyondTargetNode(interestedNodes, targetNode);
    }

    private Set<NodePath> removeForwarderPaths(Message message,
                                               Set<NodePath> differentPathsOfInterestNodes) {
        return pathHandler.removeForwarderPaths(message, differentPathsOfInterestNodes);
    }

    private long getDestination(List<NodePath> pathsGroup) {
        // We can use any nodePath to get destination, as all share
        // the same first activeConnection node
        NodePath nodePath = pathsGroup.get(0);
        for (int i = 1; i < nodePath.path.size(); i++) {
            Node node = nodePath.path.get(i);
            if (activeConnections.contains(node) || inActivationConnections.contains(node)) {
                return node.getID();
            }
        }
        //TODO deal with this
        throw new AssertException("Every possible connection is down.");
    }


    /**
     * Given a set of nodes, returns the different paths that englobe all nodes.
     * Meaning if there are only two nodes and both are on the same path, only one
     * path is returned.
     *
     * @return
     */
    private Set<NodePath> getDistinctPaths(Set<Long> liveInterestedNodes) {
        Set<NodePath> distinctPaths = new HashSet<>();

        NodePath.printPathLongs("Set of nodes", liveInterestedNodes);

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
    private Collection<List<NodePath>> groupDistinctPaths(Set<NodePath> distinctPaths) {
        Map<Long, List<NodePath>> result = new HashMap<>();
        int counter = 0; // Tracks how many lists were added

        for (NodePath path : distinctPaths) {
            int lvl = 1;
            while (lvl < path.path.size()) {
                Node node = path.path.get(lvl);
                if (downConnections.contains(node)) {
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
     * @param message The message to be sent.
     *                IMPORTANT: They should already be processed
     */
    // abstract void forwardMessages(List<Message> messages);


    // --------------------------------------------------------
    // ---------------- MESSAGE RECEIVE PROCESS ---------------
    // --------------------------------------------------------

    List<Message> receiveMessage(Message message) {
        PathHandler.Scenario scenario = pathHandler.evaluationScenario(message);

        switch (scenario) {
            case ONE:
                Message result = message;
                if (messagePublisher.belongsToGroup(message.getGroup())) {
                    result = causalityHandler.processMessage(result);
                }
                updateGcReceiveFromInformation(message);
                result = pathHandler.processMessage(result);
                return prepareMessageToForward(result);

            case TWO:
                System.exit(1);
                // messageBuffer.bufferMessage(message);
                break;

            case THREE:
                // TODO provavelmente tem de propagar as metadatas para o resto
                pathHandler.handleDuplicateMessage(message);
                break;
        }

        System.out.println("Scenario " + scenario);
        return Collections.emptyList();
    }

    // --------------
    // HELPER METHODS
    // --------------

    NodePath getFullPathOfNode(Node node) {
        return pathHandler.getFullPathOfNode(node);
    }

    NodePath getShortestPathOfNode(Node node) {
        return pathHandler.getShortestPathOfNode(node);
    }

    @Override
    public void init(Long id) {
        this.id = id;
        this.messagePublisher = new MessagePublisher(id);
        this.pathHandler = new PathHandler(id);
        this.causalityHandler = new CausalityHandler(id);
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
    public Set<NodePath> getNeighbourhood() {
        return pathHandler.getNeighbourhood();
    }

    @Override
    public void setNeighbourHood(Set<NodePath> differentPaths) {
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
    public Set<Integer> getGroups() {
        return messagePublisher.getGroups();
    }

    @Override
    public void setForwarderOfGroup(Integer group) {
        messagePublisher.setForwarderOfGroup(group);
    }

    @Override
    public void setNeighbourhoodAndPathId(NodePath path, long pathId) {
        pathHandler.setNeighbourhoodAndPathId(path, pathId);
    }

    @Override
    public void addPathIdMapping(NodePath path, long pathId) {
        pathHandler.addPathIdMapping(path, pathId);
    }

    @Override
    public void startActiveConnection(Long connectionStarter) {
        Node node = Network.get(Math.toIntExact(connectionStarter));
        startActiveConnection(node);
    }

    public void startActiveConnection(Node otherNode) {
        activeConnections.add(otherNode);
        //compareMessages(otherNode.getID());
    }

    /**
     * TODO: ADD REMOTE MESSAGES (syncing messages)
     * Important note: Currently this is instant. In the future it should
     * problaby use remote event-based calls.
     */
    /*
    private void compareMessages(Long otherNode) {
        System.out.println("I'm " + id + ", connecting to " + otherNode);
        BackendInterface backend = Initialization.servers.get(otherNode);
        CausalNeighbourBackend server = (CausalNeighbourBackend) backend;

        List<Long> msgsSentFromStarter = gcMessagesSentToEachNode.computeIfAbsent(otherNode, k -> new ArrayList<>());
        List<Long> msgsReceivedFromStarter = server.gcMessagesReceivedFromEachNode.computeIfAbsent(id, k -> new ArrayList<>());

        System.out.println("Comparing two lists:");
        System.out.println(msgsSentFromStarter);
        System.out.println(msgsReceivedFromStarter);

        for (int i = 0; i < msgsReceivedFromStarter.size(); i++) {
            long serverMsg = msgsSentFromStarter.get(i);
            long ownMsg = msgsReceivedFromStarter.get(i);

            if (serverMsg != ownMsg) {
                throw new AssertException("What the fuck?");
            }
        }

        if (msgsReceivedFromStarter.size() == msgsSentFromStarter.size()) {
            System.out.println("COMPARE OVER! GOOD JOB");
            return;
        }

        List<Long> missingMsgs = new ArrayList<>(msgsSentFromStarter);
        missingMsgs.removeAll(msgsReceivedFromStarter);
        System.out.println("(Am node " + id + ") Missing msgs for node " + otherNode + ": ");
        System.out.println(missingMsgs);

        for (Long missingMessageId : missingMsgs) {
            Message rawMessage = gcStorage.get(missingMessageId).get(otherNode);
            System.out.println("Raw message: ");
            rawMessage.printMessage();

            Message message = syncPrepareMessageToForward(rawMessage, otherNode);

            System.out.println("ADAPTED MSG:");
            message.printMessage();

            List<Message> messages = server.receiveMessage(message);
            server.frontendForwardMessage(messages);

            //TODO mensagem guardada tem tags para vários sitios, mas como
            // estamos a mandar para um nó que nao lhe interessa essas tags,
            // temos de adaptar
            // Message newMessage = new Message(rawMessage);

           // List<Message> messages = server.receiveMessage(message);
           // server.frontendForwardMessage(messages);
        }

        //System.out.println("LETS COMPARE AGAIN NOW!");
        //compareMessages(otherNode);
        // throw new AssertException("boom");

        //TODO super hardcoded e mal feito. Apenas porque é incall.
    }
    */

    @Override
    public String printStatus() {
        StringBuilder builder = new StringBuilder();
        builder.append("Node " + id + " status").append(System.lineSeparator());
        builder.append(messagePublisher.printStatus()).append(System.lineSeparator());
        builder.append(causalityHandler.printStatus()).append(System.lineSeparator());
        builder.append(pathHandler.printStatus()).append(System.lineSeparator());
        builder.append("Garbage size: ")
                .append(gcMessagesSentToEachNode.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet())
                        .size());
        return builder.toString();
    }

    // ----------------------------------
    // Methods to be used by the frontend
    // ----------------------------------

    /*
    protected List<Message> forwardToNextNeighbour(Message message, Node crashedNode) {
        System.out.println("CHANGING MESSAGE");
        Set<Long> interestedNodes = new HashSet<>();
        message.printMessage();
        for (List<MetadataEntry> vector : message.getMetadata()) {
            Node nextNode = getNextNeighbour(vector.get(vector.size() - 1), crashedNode);
            interestedNodes.add(nextNode.getID());
            vector.add(new MetadataEntry());
        }

        return prepareMessageToForward(message, interestedNodes);
    }
    */

    private Node getNextNeighbour(MetadataEntry metadataEntry, Node crashedNode) {
        long pathId = metadataEntry.getPathId();
        NodePath nodePath = pathHandler.getPathNodesFromPathId(pathId);
        for (int i = 0; i < nodePath.path.size(); i++) {
            if (nodePath.path.get(i).getID() == crashedNode.getID()) {
                //System.out.println("Returning entry " + (i+1));
                return nodePath.path.get(i + 1);
            }
        }
        System.out.println("Didn't find anything");
        return null;
    }

    protected Set<Node> connectionIsDown(Node targetNode) {
        System.out.println("(Node " + id + ") detected node " + targetNode.getID() + " down.");
        activeConnections.remove(targetNode);
        downConnections.add(targetNode);

        Node thisNode = Network.get(Math.toIntExact(id));
        Set<Node> newNeighbours = Utils
                .getNeighboursExcludingSource(targetNode, thisNode, linkablePid);
        System.out.println("(Node " + id + ") new connections: " + Utils.nodesToLongs(newNeighbours));

        inActivationConnections.addAll(newNeighbours);
        return newNeighbours;
        //newNeighbours.forEach(this::startActiveConnection);
    }

    protected void checkCrashedConnections() {
        Iterator<Node> iterator = downConnections.iterator();

        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (node.isUp()) {
                connectionIsBackOnline(node);
            }
        }
    }

    protected void connectionIsBackOnline(Node targetNode) {
        System.out.println("(Node " + id + ") detected node " + targetNode.getID() + " up.");
        startActiveConnection(targetNode);

        downConnections.remove(targetNode);

        Node thisNode = Network.get(Math.toIntExact(id));
        Set<Node> newNeighbours = Utils
                .getNeighboursExcludingSource(targetNode, thisNode, linkablePid);
        System.out.println("(Node " + id + ") removed connections: " + Utils.nodesToLongs(newNeighbours));
        activeConnections.removeAll(newNeighbours);
    }


    /// ---------------------------
    /// GARBAGE COLLECTION
    /// ---------------------------


    private Map<Long, Map<Long, Message>> gcStorage = new HashMap<>();
    private Map<Long, List<Long>> gcMessagesSentToEachNode = new HashMap<>();
    private Map<Long, List<Long>> gcMessagesReceivedFromEachNode = new HashMap<>();


    private void addGcInformation(Message message, List<NodePath> pathsGroups) {
        Set<Long> targetIds = pathsGroups.stream()
                .map(nodePath -> nodePath.fullPathSet)
                .flatMap(Collection::stream)
                .map(Node::getID)
                .collect(Collectors.toSet());


        //>> System.out.println("adding msg for " + targetIds);
        //>> message.printMessage();
        for (Long interestedNode : targetIds) {
            gcStorage.computeIfAbsent(message.getId(), k -> new HashMap<>())
                    .put(interestedNode, new Message(message));
            gcMessagesSentToEachNode.computeIfAbsent(interestedNode, k -> new ArrayList<>())
                    .add(message.getId());
        }
    }

    /**
     * Currently, this retrives the first node of each nodePath entry and updates
     * the GC map to the number of messages it received from it.
     * If this works in every scenario is yet to be understood.
     * // TODO !
     * @param message
     */
    private void updateGcReceiveFromInformation(Message message) {
        //>> System.out.println("Updating GC!");

        Set<Long> ids = new HashSet<>();

        for (List<MetadataEntry> vector : message.getMetadata()) {
            for (MetadataEntry entry : vector) {
                if (entry.getState() == MetadataEntry.State.JUMP) {
                    continue;
                }
                if (!ids.add(message.getId())) {
                    continue;
                    // throw new AssertException("What the fuck: " + ids);
                }

                NodePath nodePath = pathHandler.getPathNodesFromPathId(entry.getPathId());
                //>> nodePath.printLn("Path: ");
                Node node = nodePath.path.get(0);

                //>> System .out.println("UPDATING " + node.getID());
                gcMessagesReceivedFromEachNode
                        .computeIfAbsent(node.getID(), k -> new ArrayList<>())
                        .add(message.getId());
            }
        }
    }

    /**
     * //TODO Isto de momento só devolve as msgs que recebeu. Poderá ser optimizado um dia
     * @param sender
     * @return
     */
    protected List<Long> handleNewConnection(long sender) {
        return gcMessagesReceivedFromEachNode.getOrDefault(sender, new ArrayList<>());
    }

    protected List<Message> compareHistory(long sender, List<Long> historyFromSender) {
        List<Long> sentMessages = gcMessagesSentToEachNode.get(sender);

        System.out.println("Comparing two lists:");
        System.out.println(sentMessages);
        System.out.println(historyFromSender);

        for (int i = 0; i < historyFromSender.size(); i++) {
            long ownMsg = sentMessages.get(i);
            long serverMsg = historyFromSender.get(i);

            if (ownMsg != serverMsg) {
                throw new AssertException("What the fuck?");
            }
        }

        if (sentMessages.size() == historyFromSender.size()) {
            System.out.println("COMPARE OVER! GOOD JOB");
            return Collections.emptyList();
        }

        List<Long> missingMsgs = new ArrayList<>(sentMessages);
        missingMsgs.removeAll(historyFromSender);
        System.out.println("(Node " + id + ") Missing msgs for node " + sender + ": " + missingMsgs);

        List<Message> missingMessages = new ArrayList<>();
        for (Long missingMessageId : missingMsgs) {
            Message rawMessage = gcStorage.get(missingMessageId).get(sender);
            System.out.println("Raw message: ");
            rawMessage.printMessage();

            Message message;
            if (rawMessage.getNextDestination() != sender) {
                message = syncPrepareMessageToForward(rawMessage, sender);
            } else {
                message = rawMessage;
            }

            System.out.println("ADAPTED MSG:");
            message.printMessage();

            missingMessages.add(message);
        }

        Node senderNode = Network.get((int) sender);
        System.out.println("Adding queued messages: " + nodeMessageQueue.get(senderNode).size());
        //missingMessages.addAll(nodeMessageQueue.computeIfAbsent(senderNode, k-> new ArrayList<>()));
        nodeMessageQueue.get(senderNode).clear();
        if (!nodeMessageQueue.get(senderNode).isEmpty()) {
            throw new AssertException("List should now be empty.");
        }

        inActivationConnections.remove(senderNode);
        activeConnections.add(senderNode);
        return missingMessages;
    }
}