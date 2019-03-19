package example.myproject.server;

import example.myproject.datatypes.Message;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.List;

public class CausalNeighbourFrontend extends CausalNeighbourBackend
        implements CDProtocol, EDProtocol {

    private final int pid;
    private final int dcPid;
    private final String prefix;
    private final int publisher; // TODO isto e uma temp var

    // TODO TESTAR SE PID ESTA CORRECTO
    public CausalNeighbourFrontend(String prefix) {
        super(prefix);
        this.prefix = prefix;
        this.pid = Configuration.getPid(prefix + ".transport");
        this.dcPid = Configuration.getPid("causalneighbour");
        this.publisher = Configuration.getInt("publisher");
    }

    protected void frontendForwardMessage(List<Message> messages) {
        frontendForwardMessage(messages, dcPid);
    }

    private void frontendForwardMessage(List<Message> messages, int pid) {
        if (messages.isEmpty()) {
            return;
        }

        System.out.println("---- Forwarding messages ---");
        for (Message messageToForward : messages) {
            messageToForward.printMessage();
            frontendForwardMessage(messageToForward, pid);
        }
        System.out.println("---------------------------");
        System.out.println();
    }

    private void frontendForwardMessage(Message message, int pid) {
        Node srcNode = Network.get(Math.toIntExact(getId()));
        Node targetNode = Network.get(Math.toIntExact(message.getNextDestination()));

        if (isCrashed(targetNode)) {
            this.connectionIsDown(targetNode);
            /*
            List<Message> adaptedMessages = forwardToNextNeighbour(message, targetNode);
            for (Message adaptedMessage : adaptedMessages) {
                frontendForwardMessage(adaptedMessage, pid);
            }
            System.out.println("Returning bye!");
            */
            return;
        }
        sendMessage(srcNode, targetNode, message, pid);
    }


    /**
     * Algorithm:
     *  1) Generate message
     *  2) See which direct neighbours are interested in the message
     *      2.1) If the direct neighbours are down,
     *          check their directory neighbours (and so on until delta)
     *
     *  3) Check the nodes that are on the same path (so that they have the same
     *      metadata
     *  4) Add metadata to each message to be sent
     *  5) Send the message to each path.
     *
     */
    @Override
    public void nextCycle(Node node, int protocolID) {

        if (isCrashed(node)) {
            return;
        }

        checkCrashedConnections();

        //TODO TIRAR ISTO NÉ
        if (node.getID() != publisher) {
            return;
        }
        // System.out.println("PUBLISHER: " + node.getID());

        List<Message> newMessages = this.publishMessage();

        frontendForwardMessage(newMessages, protocolID);
    }

    private boolean isCrashed(Node node) {
        return !node.isUp();
    }


    private void sendMessage(Node src, Node dst, Object msg, int pid) {
        System.out.println("Sending from " + src.getID() + " to " + dst.getID());
        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
        if (event instanceof Message) {
            Message message = (Message) event;
            System.out.println("---------------------");
            System.out.println("Received message: (Node " + node.getID() + ")");
            message.printMessage();
            List<Message> messagesToForward = receiveMessage(message);
            frontendForwardMessage(messagesToForward, pid);
        } else {
            throw new RuntimeException("Unknown message type.");
        }
    }

    @Override
    public Object clone() {
        return new CausalNeighbourFrontend(prefix);
    }

}