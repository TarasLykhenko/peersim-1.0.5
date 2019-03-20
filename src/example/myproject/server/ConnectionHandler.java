package example.myproject.server;

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

    private final long id;
    private final PathHandler pathHandler;

    private Map<Long, Map<Long, Message>> gcStorage = new HashMap<>();

    private Map<Long, List<Long>> gcMessagesSentToEachNode = new HashMap<>();
    private Map<Long, List<Long>> gcMessagesReceivedFromEachNode = new HashMap<>();

    private Set<Node> activeConnections = new HashSet<>();

    Set<Node> downConnections = new HashSet<>();
    private Set<Node> inActivationConnections = new HashSet<>();

    public ConnectionHandler(long id, PathHandler pathHandler, CausalNeighbourFrontend frontend) {
        this.id = id;
        this.pathHandler = pathHandler;
    }

    private void addGcInformation(Message message) {

        List<NodePath> nodePaths = new ArrayList<>();
        for (List<MetadataEntry> vector : message.getMetadata()) {
            MetadataEntry lastNonNullEntry = Message.getLastNonNullEntry(vector);
            nodePaths.add(pathHandler.getShortestPathOfNode(lastNonNullEntry.getPathId()));
        }


        Set<Long> targetIds = nodePaths.stream()
                .map(nodePath -> nodePath.fullPathSet)
                .flatMap(Collection::stream)
                .map(Node::getID)
                .collect(Collectors.toSet());

        System.out.println("(DEBUG N " + id + ") adding msg for " + targetIds);
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
    void updateGcReceiveFromInformation(Message message) {
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
    List<Long> handleNewConnection(long sender) {
        return gcMessagesReceivedFromEachNode.getOrDefault(sender, new ArrayList<>());
    }

    List<Message> compareHistory(long sender, List<Long> historyFromSender) {
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

        inActivationConnections.remove(senderNode);
        activeConnections.add(senderNode);
        return missingMessages;
    }



    Set<Node> connectionIsDown(Node targetNode) {
        System.out.println("(Node " + id + ") detected node " + targetNode.getID() + " down.");
        activeConnections.remove(targetNode);
        downConnections.add(targetNode);

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
            if (node.isUp()) {
                connectionIsBackOnline(node);
            }
        }
    }

    void connectionIsBackOnline(Node targetNode) {
        System.out.println("(Node " + id + ") detected node " + targetNode.getID() + " up.");
        startActiveConnection(targetNode);

        downConnections.remove(targetNode);

        Node thisNode = Network.get(Math.toIntExact(id));
        Set<Node> newNeighbours = Utils
                .getNeighboursExcludingSource(targetNode, thisNode);
        System.out.println("(Node " + id + ") removed connections: " + Utils.nodesToLongs(newNeighbours));
        activeConnections.removeAll(newNeighbours);
    }

    public void startActiveConnection(Long connectionStarter) {
        Node node = Network.get(Math.toIntExact(connectionStarter));
        startActiveConnection(node);
    }

    void startActiveConnection(Node otherNode) {
        activeConnections.add(otherNode);
        //compareMessages(otherNode.getID());
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
        differentMessagesToSend.forEach(this::addGcInformation);
        differentMessagesToSend.forEach(this::checkIfDestinationIsCrashed);
        addMessagesToConnectionQueues(differentMessagesToSend);
    }

    private void checkIfDestinationIsCrashed(Message message) {
        long destination = message.getNextDestination();
        Node dstNode = Network.get((int) destination);
        if (Utils.isCrashed(dstNode)) {
            //TODO prestar atenção: connectionIsDown faz tudp de uma só vez. É preciso fazer agora manualmente
            Set<Node> newNeighbours = this.connectionIsDown(targetNode);
            for (Node newNeighbour : newNeighbours) {
                StartConnectionMsg msg = new StartConnectionMsg(id, newNeighbour.getID());
                sendMessage(srcNode, newNeighbour, msg, pid);
            }

            return;
            /*
            List<Message> adaptedMessages = forwardToNextNeighbour(message, targetNode);
            for (Message adaptedMessage : adaptedMessages) {
                frontendForwardMessage(adaptedMessage, pid);
            }
            System.out.println("Returning bye!");
            */
        }
    }

    boolean containsConnection(Node node) {
        return activeConnections.contains(node) || inActivationConnections.contains(node);
    }

    String printStatus() {
        StringBuilder builder = new StringBuilder();
        builder.append("Up connections: ").append(activeConnections).append(System.lineSeparator());
        builder.append("Down connections: ").append(downConnections).append(System.lineSeparator());
        builder.append("Syncing connections: ").append(inActivationConnections).append(System.lineSeparator());
        builder.append("Garbage size: ").append(gcStorage.size()).append(System.lineSeparator());
        return builder.toString();
    }
}
