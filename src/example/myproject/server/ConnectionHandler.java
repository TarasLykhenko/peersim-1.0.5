package example.myproject.server;

import example.myproject.Statistics;
import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import example.myproject.datatypes.MetadataEntry;
import example.myproject.datatypes.NodePath;
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

/**
 * Handles the garbage collection and existing connections
 */
public class ConnectionHandler {

    private final PathHandler pathHandler;
    private final CausalNeighbourBackend backend;
    private final long id;

    /**
     * Per each message to be sent, the id of the message is mapped to the
     * different destination ids and version of the message that is to be sent
     *
     * (Each different message to be sent has different metadata)
     */
    private Map<Long, Map<Long, Message>> gcStorage = new HashMap<>();

    private Map<Long, List<Long>> gcMessagesSentToEachNode = new HashMap<>();
    private Map<Long, List<Long>> gcMessagesReceivedFromEachNode = new HashMap<>();

    private Set<Node> activeConnections = new HashSet<>();
    private Set<Node> downConnections = new HashSet<>();
    private Set<Node> inActivationConnections = new HashSet<>();

    ConnectionHandler(long id, CausalNeighbourBackend backend) {
        this.id = id;
        this.backend = backend;
        this.pathHandler = backend.getPathHandler();
    }

    // TODO Perceber de novo porque é que há ids duplicados
    private void addOutgoingGcInformation(Message message) {

        List<NodePath> nodePaths = new ArrayList<>();

        for (List<MetadataEntry> vector : message.getMetadata()) {
            MetadataEntry lastNonNullEntry = Message.getLastNonNullEntry(vector);
            nodePaths.add(pathHandler.getPathNodesFromPathId(lastNonNullEntry.getPathId()));
        }

        Set<Long> targetIds = nodePaths.stream()
                .map(nodePath -> nodePath.fullPathSet)
                .flatMap(Collection::stream)
                .map(Node::getID)
                .collect(Collectors.toSet());


        if (Utils.DEBUG_V) {
            System.out.println("(DEBUG N " + id + ") adding msg for " + targetIds);
        }
        //>> message.printMessage();

        for (Long interestedNode : targetIds) {
            addMessageStatistics(message);

            Map<Long, Message> msgsToEachNode = gcStorage.computeIfAbsent(message.getId(), k -> new HashMap<>());
            if (msgsToEachNode.containsKey(interestedNode) && msgsToEachNode.get(interestedNode).fullyEqual(message)) {
                throw new AssertException("what the fuck");
            }

            gcStorage.computeIfAbsent(message.getId(), k -> new HashMap<>())
                    .put(interestedNode, new Message(message));

            List<Long> msgsSent = gcMessagesSentToEachNode.computeIfAbsent(interestedNode, k -> new ArrayList<>());

            //TODO Isto é O(N) :^(
            if (msgsSent.stream().anyMatch(aLong -> aLong.equals(message.getId()))) {
                System.out.println("Repeated value! " + message.getId());
                continue;
            }

            msgsSent.add(message.getId());
        }
    }


    /**
     * Given a message that is received, all metadata entries up to this node
     * should have the same path (as a message can only arrive to a node through
     * a path), therefore we only want to update the "ReceiveFrom" DataStructure
     * with the nodes that are from that path up to the current node and not further
     * than that.
     *
     * @param message
     */
    void updateGcReceiveFromInformation(Message message) {
        //>> System.out.println("Updating GC!");


        // Get the vector with the biggest path information
        List<MetadataEntry> vector = message.getMetadata().get(0);
        for (List<MetadataEntry> otherVector : message.getMetadata()) {
            if (otherVector.size() > vector.size()) {
                throw new AssertException("This is odd. The vectors should all have the same size.");
            }
        }

        MetadataEntry firstNonJumpEntry = null;
        for (MetadataEntry metadataEntry : vector) {
            if (metadataEntry.getState() == MetadataEntry.State.JUMP) {
                continue;
            }
            firstNonJumpEntry = metadataEntry;
            break;
        }

        NodePath nodePath = pathHandler.getPathNodesFromPathId(firstNonJumpEntry.getPathId());
        for (Node node : nodePath.fullPathSet) {
            if (node.getID() == id) {
                return;
            }

            gcMessagesReceivedFromEachNode
                    .computeIfAbsent(node.getID(), k -> new ArrayList<>())
                    .add(message.getId());
        }

        for (int i = 1; i < vector.size(); i++) {
            if (vector.get(i).getState() == MetadataEntry.State.JUMP) {
                continue;
            }

            NodePath nextNodePath = pathHandler.getPathNodesFromPathId(vector.get(i).getPathId());
            Node lastNode = Utils.getLastEntry(nextNodePath.path);

            if (lastNode.getID() == id) {
                System.out.println("Final return");
                return;
            }

            gcMessagesReceivedFromEachNode
                    .computeIfAbsent(lastNode.getID(), k -> new ArrayList<>())
                    .add(message.getId());
        }
    }

    //TODO Isto de momento só devolve as msgs que recebeu. Poderá ser optimizado um dia
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
        return gcMessagesReceivedFromEachNode.getOrDefault(sender, new ArrayList<>());
    }

    /**
     * Part of the synch protocol. This is the 3rd of the 3rd step.
     *
     * 1st step: Server A sends ConnectionStart message to server B.
     * 2nd step: Server B receives the message, sends the messages it received from A.
     * 3rd step: Server A sends the missing messages from A to B.
     *
     * @param sender            The server that is the target of the synch protocol
     * @param historyFromSender The history from the target server
     * @return All messages that are missing in the target server.
     */
    List<Message> compareHistory(long sender, List<Long> historyFromSender) {
        List<Long> sentMessages = gcMessagesSentToEachNode.get(sender);

        if (Utils.DEBUG_V) {
            System.out.println("Comparing two lists:");
            System.out.println(sentMessages);
            System.out.println(historyFromSender);
        }


        for (int i = 0; i < historyFromSender.size(); i++) {
            long ownMsg = sentMessages.get(i);
            long serverMsg = historyFromSender.get(i);

            if (ownMsg != serverMsg) {
                System.out.println("Comparing two lists: (am " + id + ", target is " + sender + ")");
                System.out.println("Msgs sent from " + id + " to " + sender + ": " + sentMessages);
                System.out.println("Msgs received by " + sender + " from " + id + ": " + historyFromSender);

                throw new AssertException("What the fuck? (Nodes " + id + " and " + sender + ")");
            }
        }

        if (sentMessages.size() == historyFromSender.size()) {
            System.out.println("COMPARE OVER! GOOD JOB");
            return Collections.emptyList();
        }

        List<Long> missingMsgs = new ArrayList<>(sentMessages);
        missingMsgs.removeAll(historyFromSender);

        if (Utils.DEBUG) {
            System.out.println("(Node " + id + ") Missing msgs for node " + sender + ": " + missingMsgs);
        }

        List<Message> missingMessages = new ArrayList<>();
        for (Long missingMessageId : missingMsgs) {
            Message rawMessage = gcStorage.get(missingMessageId).get(sender);

            if (Utils.DEBUG_V) {
                System.out.println("Raw message: ");
                rawMessage.printMessage();
            }


            Message message;
            if (rawMessage.getNextDestination() != sender) {
                message = backend.syncPrepareMessageToForward(rawMessage, sender);
            } else {
                message = rawMessage;
            }

            if (Utils.DEBUG_V) {
                System.out.println("ADAPTED MSG:");
                message.printMessage();
            }

            missingMessages.add(message);
        }

        Node senderNode = Network.get((int) sender);

        inActivationConnections.remove(senderNode);
        activeConnections.add(senderNode);
        return missingMessages;
    }

    private Set<Node> connectionIsDown(Node targetNode) {
        System.out.println("(Node " + id + ") detected node " + targetNode.getID() + " down.");
        activeConnections.remove(targetNode);
        downConnections.add(targetNode);

        NodePath path = pathHandler.getShortestPathOfNode(targetNode);
        if (path.path.size() > Utils.DELTA + 1) {
            path.printLn("This is too far. Not returning any neighbours.");
            return Collections.emptySet();
        }

        Node thisNode = Network.get(Math.toIntExact(id));
        Set<Node> newNeighbours = Utils
                .getNeighboursExcludingSource(targetNode, thisNode);
        System.out.println("(Node " + id + ") new connections: " + Utils.nodesToLongs(newNeighbours));

        inActivationConnections.addAll(newNeighbours);
        return newNeighbours;
        //newNeighbours.forEach(this::startActiveConnection);
    }

    void checkCrashedConnections() {
        Iterator<Node> iterator = downConnections.iterator();

        while (iterator.hasNext()) {
            Node node = iterator.next();

            if (!Utils.isCrashed(node)) {
                connectionIsBackOnline(node);
            }
        }
    }

    private void connectionIsBackOnline(Node targetNode) {
        System.out.println("(Node " + id + ") detected node " + targetNode.getID() + " up.");

        inActivationConnections.add(targetNode);
        backend.startConnection(targetNode.getID());

        downConnections.remove(targetNode);

        Node thisNode = Network.get(Math.toIntExact(id));
        Set<Node> newNeighbours = Utils
                .getNeighboursExcludingSource(targetNode, thisNode);
        System.out.println("(Node " + id + ") removed connections: " + Utils.nodesToLongs(newNeighbours));
        activeConnections.removeAll(newNeighbours);
    }

    void startActiveConnection(Node otherNode) {
        activeConnections.add(otherNode);
        //backend.startActiveConnection(otherNode.getID());
    }

    /**
     * This added messages to a connection queue. However, since we know we cannot
     * send the message directly before the connection is stable (or we can break FIFO),
     * we simply don't send the message and then send it when the connection is synching
     * with the compareMessages routine.
     *
     * @param differentMessagesToSend
     */
    private void addMessagesToConnectionQueues(List<Message> differentMessagesToSend) {
        Iterator<Message> iterator = differentMessagesToSend.iterator();
        while (iterator.hasNext()) {
            Message message = iterator.next();
            long destination = message.getNextDestination();
            Node dstNode = Network.get((int) destination);
            if (inActivationConnections.contains(dstNode)) {
                //>> System.out.println("Adding!");
                // nodeMessageQueue.computeIfAbsent(dstNode, k -> new ArrayList<>()).add(message);
                //>> System.out.println("Removing msg to dst " + destination);
                iterator.remove();
            }
        }
    }

    /**
     * This must be called before sending messages to the network.
     *
     * 1) Adds outgoing messages to the GC queue
     * 2) Checks if any target is down, and if so then starts
     * 3) Removes messages to connections that are synching
     *
     * @param differentMessagesToSend
     */
    void handleOutgoingMessages(List<Message> differentMessagesToSend) {
        differentMessagesToSend.forEach(this::addOutgoingGcInformation);
        differentMessagesToSend.forEach(this::checkIfDestinationIsCrashed);
        addMessagesToConnectionQueues(differentMessagesToSend);
    }

    private void checkIfDestinationIsCrashed(Message message) {
        long destination = message.getNextDestination();
        Node dstNode = Network.get((int) destination);

        if (Utils.isCrashed(dstNode)) {
            Set<Node> newNeighbours = connectionIsDown(dstNode);
            for (Node newNeighbour : newNeighbours) {
                backend.startConnection(newNeighbour.getID());
            }
        }
    }

    boolean containsConnection(Node node) {
        return activeConnections.contains(node) || inActivationConnections.contains(node);
    }

    Set<Node> getActiveConnections() {
        return activeConnections;
    }

    Set<Node> getDownConnections() {
        return downConnections;
    }

    String printStatus() {
        Set<Long> upConnections = activeConnections.stream().map(Node::getID).collect(Collectors.toSet());
        Set<Long> crashedConnections = downConnections.stream().map(Node::getID).collect(Collectors.toSet());

        return "Up connections: " + upConnections + System.lineSeparator() +
                "Down connections: " + crashedConnections + System.lineSeparator() +
                "Syncing connections: " + inActivationConnections + System.lineSeparator() +
                "Garbage size: " + gcStorage.size() + System.lineSeparator();
    }

    private void addMessageStatistics(Message message) {
        Set<MetadataEntry> differentEntries = new HashSet<>();
        long totalSize = message.getMetadataSize();
        for (List<MetadataEntry> vector : message.getMetadata()) {
            differentEntries.addAll(vector);
        }

        long numberRepetitions = totalSize - differentEntries.size();
        if (numberRepetitions > Statistics.NUMBER_REPETITIONS) {
            Statistics.NUMBER_REPETITIONS = numberRepetitions;
            Statistics.MESSAGE = message;
        }
    }
}
