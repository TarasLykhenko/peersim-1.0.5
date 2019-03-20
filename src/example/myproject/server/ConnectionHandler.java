package example.myproject.server;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles the garbage collection and existing connections
 */
public class ConnectionHandler {

    private final long id;

    public ConnectionHandler(long id) {
        this.id = id;
    }

    private Map<Long, Map<Long, Message>> gcStorage = new HashMap<>();
    private Map<Long, List<Long>> gcMessagesSentToEachNode = new HashMap<>();
    private Map<Long, List<Long>> gcMessagesReceivedFromEachNode = new HashMap<>();


    private void addGcInformation(Message message, List<NodePath> pathsGroups) {
        Set<Long> targetIds = pathsGroups.stream()
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

        inActivationConnections.remove(senderNode);
        activeConnections.add(senderNode);
        return missingMessages;
    }
}
