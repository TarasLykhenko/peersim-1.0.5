package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.NodePath;
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
public class CausalNeighbourBackend implements BackendInterface {


    /**
     * The nodeID of the node this specific protocol instance belongs to.
     */
    protected long id;

    private CausalNeighbourFrontend frontend;
    private MessagePublisher messagePublisher;
    private PathHandler pathHandler;
    private CausalityHandler causalityHandler;
    private ConnectionHandler connectionHandler;

    public CausalNeighbourBackend(CausalNeighbourFrontend frontend) {
        this.frontend = frontend;
    }

    // ----------
    // PUBLIC API
    // ----------

    /**
     * Publishes a new message.
     *
     * This message may be cloned to be sent to different nodes.
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

    /**
     * Receives a message.
     *
     * This message may then be forwarder to other nodes.
     * @param message
     * @return
     */
    List<Message> receiveMessage(Message message) {
        PathHandler.Scenario scenario = pathHandler.evaluationScenario(message);
        List<Message> resultList = Collections.emptyList();

        switch (scenario) {
            case ONE:
                Message result = message;
                if (messagePublisher.belongsToGroup(message.getGroup())) {
                    result = causalityHandler.processMessage(result);
                }
                connectionHandler.updateGcReceiveFromInformation(message);
                result = pathHandler.processMessage(result);
                resultList = prepareMessageToForward(result);
                break;

            case TWO:
                throw new AssertException("This scenario is impossible.");
                // messageBuffer.bufferMessage(message);
                // break;

            case THREE:
                // TODO provavelmente tem de propagar as metadatas para o resto
                pathHandler.handleDuplicateMessage(message);
                throw new AssertException("TODO must implement this scenario!");
                // break;
        }

        System.out.println("Scenario " + scenario);

        return resultList;
    }

    /**
     * Part of the synch protocol. This is the 2nd of the 3rd step.
     *
     * 1st step: Server A sends ConnectionStart message to server B.
     * 2nd step: Server B receives the message, sends the messages it received from A.
     * 3rd step: Server A sends the missing messages from A to B.
     *
     * @param sender The server that started the synch protocol
     * @return The list of ids of the missing messages
     */
    List<Long> handleNewConnection(long sender) {
        return this.connectionHandler.handleNewConnection(sender);
    }

    /**
     * Part of the synch protocol. This is the 3rd of the 3rd step.
     *
     * 1st step: Server A sends ConnectionStart message to server B.
     * 2nd step: Server B receives the message, sends the messages it received from A.
     * 3rd step: Server A sends the missing messages from A to B.
     *
     * @param sender The server that is the target of the synch protocol
     * @param historyFromSender The history from the target server
     * @return All messages that are missing in the target server.
     */
    List<Message> compareHistory(long sender, List<Long> historyFromSender) {
        return this.connectionHandler.compareHistory(sender, historyFromSender);
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
            differentMessagesToSend.add(messageToSend);
        }

        connectionHandler.handleOutgoingMessages(differentMessagesToSend);

        return differentMessagesToSend;
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
            if (connectionHandler.containsConnection(node)) {
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
                if (connectionHandler.downConnections.contains(node)) {
                    System.out.println("SAD!");
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


    // --------------
    // HELPER METHODS
    // --------------

    NodePath getShortestPathOfNode(Node node) {
        return pathHandler.getShortestPathOfNode(node);
    }

    @Override
    public void init(Long id) {
        this.id = id;
        this.messagePublisher = new MessagePublisher(id);
        this.pathHandler = new PathHandler(id);
        this.causalityHandler = new CausalityHandler(id);
        this.connectionHandler = new ConnectionHandler(id, pathHandler, frontend);
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

    @Override
    public String printStatus() {
        StringBuilder builder = new StringBuilder();
        String backendStatus = "Node " + id + " status";
        builder.append(backendStatus).append(System.lineSeparator());
        builder.append(messagePublisher.printStatus()).append(System.lineSeparator());
        builder.append(causalityHandler.printStatus()).append(System.lineSeparator());
        builder.append(pathHandler.printStatus()).append(System.lineSeparator());
        builder.append(connectionHandler.printStatus()).append(System.lineSeparator());
        return builder.toString();
    }

    // ----------------------------------
    // Methods to be used by the frontend
    // ----------------------------------
}