package example.myproject.server;

import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;
import peersim.config.Configuration;

import java.util.HashMap;
import java.util.Map;

public class CausalityHandler {

    private final long id;
    /**
     * This map is used to store the messages received from each node in the system
     * and also to store all destinations and msg number, in order to test causality.
     *
     * The idea behind this is Causal Barriers but without any optimizations.
     *
     *
     *
     *
     * This map is used to store the number of messages received
     * from each node in the entire system.
     *
     * It is used to test causality.
     *
     * When a message is published by a node, the node sends as "data" the
     * number of messages it has seen from other nodes. When a target node
     * receives that message, it compares with the number of messages it has
     * received from those nodes. If any of the entries of t his node is smaller
     * and the message is delivered then causality has been broken.
     *
     * The mapping is Long:Publisher to Map of Long:TargetNode - Integer: Latest msg sent
     */
    private Map<Long, Map<Long, Integer>> publisherMessages = new HashMap<>();

    private final boolean VERIFY_CAUSALITY_TEMP_VAR;

    public CausalityHandler(long id) {
        this.id = id;
        VERIFY_CAUSALITY_TEMP_VAR = Configuration.getBoolean("verify-causality");
    }

    void processMessage(Message message) {
        deliverMessage(message);
    }

    private void deliverMessage(Message message) {

        // First check FIFO
        Long sender = message.getSender();
        Integer msgValueForThisNode = message.getData().get(id);

        int currentNodeEntry = publisherMessages
                .computeIfAbsent(sender, k -> new HashMap<>())
                .getOrDefault(id, 0);

        if (msgValueForThisNode != currentNodeEntry + 1) {
            throw new AssertException("FIFO broken. Sender: " + sender +
                    ", got " + currentNodeEntry + ", this is " + msgValueForThisNode + ".");
        }

        // Then check Causality

        if (VERIFY_CAUSALITY_TEMP_VAR) {
            Map<Long, Map<Long, Integer>> messagePast = message.getPast();

            if (Utils.DEBUG_V) {
                printPast();
                message.printPast();
            }

            for (Long publisher : messagePast.keySet()) {
                if (publisher == id) {
                    continue;
                }

                Map<Long, Integer> msgsSentByPublisher = messagePast.get(publisher);
                Integer msgPastValue = msgsSentByPublisher.get(id);
                Integer storedValue = publisherMessages
                        .computeIfAbsent(publisher, k -> new HashMap<>())
                        .getOrDefault(id, 0);

                if (msgPastValue > storedValue) {
                    printPast();
                    message.printPast();
                    throw new AssertException("Causality broken. Should have received " + msgPastValue +
                            " from " + publisher + ", got " + storedValue);
                }
            }
        }


        // Then update our past with the targets of the current message

        for (long target : message.getData().keySet()) {
            int value = message.getData().get(target);
            publisherMessages.computeIfAbsent(sender, k -> new HashMap<>())
                    .put(target, value);
        }
    }


    void addPublisherState(Message freshMessage) {
        freshMessage.addPublisherState(publisherMessages);
    }

    String printStatus() {
        StringBuilder builder = new StringBuilder();
        builder.append("Messages from: ").append(System.lineSeparator());
        for (Long publisher : publisherMessages.keySet()) {
            Map<Long, Integer> messages = publisherMessages.get(publisher);
            Integer value = messages.get(id);

            builder.append(publisher)
                    .append(" : ")
                    .append(value)
                    .append(System.lineSeparator());
        }

        return builder.toString();
    }

    private void printPast() {
        System.out.println("Node " + id + " past: ");
        for (Long publisher : publisherMessages.keySet()) {
            Map<Long, Integer> msgsSent = publisherMessages.get(publisher);
            System.out.println(publisher + " >> " + msgsSent);
        }
    }
}
