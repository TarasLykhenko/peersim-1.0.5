package example.capstone;

import example.capstone.datatypes.UpdateMessage;
import peersim.config.FastConfig;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class BrokerProtocol implements EDProtocol {

    private int counter;
    private int nodeId;

    /**
     * There are two scenarios for a broker
     *
     *  Scenario 1: Update is received from downstream:
     *      1) First, update the broker's entry
     *      2) Check if children brokers want the update
     *      3) Check if the parent wants the update
     *
     *  Scenario 2: Update is received from upstream:
     *      1) Check which children want the update.
     */
    @Override
    public void processEvent(Node node, int pid, Object event) {
        if (event instanceof UpdateMessage) {
            UpdateMessage updateMessage = (UpdateMessage) event;

            TreeOverlay treeOverlay = GroupsManager.getInstance().getTreeOverlay();
            if (treeOverlay.nodeIsParent((int) node.getID(), updateMessage.getLastSender())) {
                handleMessageFromUpstream(node, updateMessage, pid);
            } else {
                handleMessageFromDownstream(node, updateMessage, pid);
            }
        } else {
            throw new RuntimeException("Unknown message type: " + event.getClass().getSimpleName());
        }
    }

    private void handleMessageFromDownstream(Node src, UpdateMessage updateMessage, int pid) {
        updateMessage.updateVectorClockEntry(nodeId, counter++);

        for (Node child : getInterestedChildren(src, updateMessage.getKey())) {
            UpdateMessage clonedUpdateMessage = new UpdateMessage(updateMessage, this.nodeId);
            sendMessage(src, child, clonedUpdateMessage, pid);
        }

        Long parent = GroupsManager.getInstance().getTreeOverlay().getParent(src.getID());

        if (nodeIsInterested(parent, updateMessage.getKey())) {
            UpdateMessage clonedUpdateMessage = new UpdateMessage(updateMessage, this.nodeId);
            sendMessage(src, getNodeFromId(parent), clonedUpdateMessage, pid);
        }
    }

    private void handleMessageFromUpstream(Node src, UpdateMessage updateMessage, int pid) {
        for (Node child : getInterestedChildren(src, updateMessage.getKey())) {
            UpdateMessage clonedUpdateMessage = new UpdateMessage(updateMessage, this.nodeId);
            sendMessage(src, child, clonedUpdateMessage, pid);
        }
    }

    private Node getNodeFromId(long nodeId) {
        return Network.get(Math.toIntExact(nodeId));
    }

    private Set<Node> getInterestedChildren(Node src, int key) {
        Set<Node> interestedChildren = new HashSet<>();
        TreeOverlay treeOverlay = GroupsManager.getInstance().getTreeOverlay();
        Collection<Long> children = treeOverlay.getChildren(src.getID());
        for (Long childId : children) {
            if (nodeIsInterested(childId, key)) {
                interestedChildren.add(Network.get(Math.toIntExact(childId)));
            }
        }

        return interestedChildren;
    }

    private boolean nodeIsInterested(long nodeId, int key) {
        return GroupsManager.getInstance().isInterested(nodeId, key);
    }


    private void sendMessage(Node src, Node dst, Object msg, int pid) {
        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

    @Override
    public Object clone() {
        return new BrokerProtocol();
    }
}
