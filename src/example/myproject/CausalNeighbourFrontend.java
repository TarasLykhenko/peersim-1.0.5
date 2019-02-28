package example.myproject;

import example.myproject.datatypes.Message;
import peersim.cdsim.CDProtocol;
import peersim.config.FastConfig;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CausalNeighbourFrontend extends CausalNeighbourBackend
        implements CDProtocol, EDProtocol {

    public CausalNeighbourFrontend(String prefix) {
        super(prefix);
    }

    @Override
    Message forwardMessage(Message message) {
        return null;
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

        Message newMessage = this.publishMessage();

        if (newMessage == null) {
            return;
        }

        Set<Long> interestedNodes = getInterestedNodes(newMessage);
        Set<Long> liveInterestedNodes = getLiveInterestedNodes(interestedNodes);
        Set<List<Node>> differentPathsOfInterestNodes = getDistinctPaths(liveInterestedNodes);

        for (List<Node> path : differentPathsOfInterestNodes) {
            Message messageToSend = new Message(newMessage);
            appendMetadataToMessage(messageToSend, path);
            sendMessage(node, path.get(0), messageToSend, protocolID);
        }
    }

    /**
     * Given a set of nodes, returns the different paths that englobe all nodes.
     * Meaning if there are only two nodes and both are on the same path, only one
     * path is returned.
     *
     * @return
     */
    private Set<List<Node>> getDistinctPaths(Set<Long> liveInterestedNodes) {
        Set<List<Node>> distinctPaths = new HashSet<>();

        String collect =
                liveInterestedNodes.stream().map(Objects::toString).collect(Collectors.joining(" "));
        System.out.println("Set of nodes: " + collect);

        for (Long nodeId : liveInterestedNodes) {
            Node node = Network.get(Math.toIntExact(nodeId));
            List<Node> fullPathOfNode = getFullPathOfNode(node);
            distinctPaths.add(fullPathOfNode);
        }

        for (List<Node> path : distinctPaths) {
            String pathCollect =
                    path.stream()
                            .map(Node::getID)
                            .map(Objects::toString)
                            .collect(Collectors.joining(" "));
            System.out.println("Distinct path: " + pathCollect);
        }
        return distinctPaths;
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
        return new CausalNeighbourFrontend("");
    }

}