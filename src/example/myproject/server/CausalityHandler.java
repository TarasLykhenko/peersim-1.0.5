package example.myproject.server;

import example.myproject.datatypes.AssertException;
import example.myproject.datatypes.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CausalityHandler {

    /**
     *  This map is used to store the number of messages received
     *  from each node in the entire system.
     *
     *  It is used to test causality.
     *
     *  When a message is published by a node, the node sends as "data" the
     *  number of messages it has seen from other nodes. When a target node
     *  receives that message, it compares with the number of messages it has
     *  received from those nodes. If any of the entries of this node is smaller
     *  and the message is delivered then causality has been broken.
     */
    private Map<Long, Integer> publisherMessages = new HashMap<>();

    public List<Message> processMessages(List<Message> messages) {

        // Before returning, verify that they are causally correct
        deliverMessages(null); // TODO
        return null;
    }

    // TODO ver se isto está bem feito
    public void deliverMessages(List<Message> causallyApprovedMessages) {
        for (Message message : causallyApprovedMessages) {

            Map<Long, Integer> messageCausality = message.getData();

            for (Map.Entry<Long, Integer> entry : messageCausality.entrySet()) {
                Long causalNode = entry.getKey();
                Integer causalNodeValue = entry.getValue();

                Integer currentNodeEntry = publisherMessages
                        .getOrDefault(causalNode, 0);

                // TODO provavelmente
                if (currentNodeEntry  > causalNodeValue) {
                    throw new AssertException("Causality has been broken.");
                }
            }
        }
    }


    public void addPublisherState(Message freshMessage) {
        freshMessage.addPublisherState(new HashMap<>(publisherMessages));
    }
}
