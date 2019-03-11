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
    private final String prefix;

    // TODO TESTAR SE PID ESTA CORRECTO
    public CausalNeighbourFrontend(String prefix) {
        super(prefix);
        this.prefix = prefix;
        this.pid = Configuration.getPid(prefix + ".transport");
    }


    @Override
    void forwardMessages(List<Message> messages) {
        for (Message message : messages) {
            frontendForwardMessage(message);
        }
    }

    private void frontendForwardMessage(Message message) {
        Node srcNode = Network.get(Math.toIntExact(getId()));
        Node targetNode = Network.get(Math.toIntExact(message.getNextDestination()));
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

        //TODO TIRAR ISTO NÉ
        if (node.getID() != 2) {
            return;
        }
        System.out.println("PUBLISHER: " + node.getID());

        List<Message> newMessage = this.publishMessage();

        if (newMessage == null) {
            return;
        }

        for (Message destinationMsg : newMessage) {
            Long nextDestinationId = destinationMsg.getNextDestination();
            Node nextDestinationNode = Network.get(Math.toIntExact(nextDestinationId));
            sendMessage(node, nextDestinationNode, destinationMsg, protocolID);
        }
    }


    private void sendMessage(Node src, Node dst, Object msg, int pid) {
        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

    private void sendMessage(Long srcLong, Long dstLong, Object msg, int pid) {
        Node src = Network.get(Math.toIntExact(srcLong));
        Node dst = Network.get(Math.toIntExact(dstLong));

        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
        if (event instanceof Message) {
            Message message = (Message) event;
            receiveMessage(message);
        } else {
            throw new RuntimeException("Unknown message type.");
        }
    }

    @Override
    public Object clone() {
        return new CausalNeighbourFrontend(prefix);
    }

}