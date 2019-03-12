package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.NodePath;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.Collection;
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

        System.out.println("Publishing a message!");
        return prepareMessageToForward(freshMessage);
    }

    private List<Message> prepareMessageToForward(Message message) {
        Set<Long> interestedNodes = getInterestedNodes(message);
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

        return differentMessagesToSend;
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
            if (activeConnections.contains(node)) {
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
            NodePath fullPathOfNode = getFullPathOfNode(node);
            System.out.println("Checking " + nodeId + " - " + fullPathOfNode);

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
                    System.out.println("Removing previous existing path (" + distinctPath + ", " + fullPathOfNode + ")");
                    distinctPaths.remove(distinctPath);
                    break;
                }
            }

            if (!setContainsSubpath) {
                System.out.println("Adding " + fullPathOfNode.getPathString());
                distinctPaths.add(fullPathOfNode);
            }
        }


        for (NodePath path : distinctPaths) {
            path.printLn("Distinct path");
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
     * @param messages The list of messages that are to be sent.
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
                Message causallyApprovedMessage = causalityHandler.processMessage(message);
                Message processedMessage = pathHandler.processMessage(causallyApprovedMessage);
                return prepareMessageToForward(processedMessage);

            case TWO:
                System.exit(1);
                // messageBuffer.bufferMessage(message);
                break;

            case THREE:
                // TODO provavelmente tem de propagar as metadatas para o resto
                pathHandler.handleDuplicateMessage(message);
                break;
        }
        return null;
    }

    // --------------
    // HELPER METHODS
    // --------------

    NodePath getFullPathOfNode(Node node) {
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
        activeConnections.add(node);
    }

    @Override
    public String printStatus() {
        StringBuilder builder = new StringBuilder();
        builder.append("Node " + id + " status").append(System.lineSeparator());
        builder.append(messagePublisher.printStatus()).append(System.lineSeparator());
        builder.append(causalityHandler.printStatus()).append(System.lineSeparator());
        builder.append(pathHandler.printStatus()).append(System.lineSeparator());
        return builder.toString();
    }
}