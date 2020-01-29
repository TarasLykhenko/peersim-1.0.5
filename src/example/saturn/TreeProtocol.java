package example.saturn;

import example.common.MigrationMessage;
import example.common.PointToPointTransport;
import example.common.datatypes.Operation;
import example.saturn.datatypes.message.types.Message;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class TreeProtocol extends StateTreeProtocolInstance
        implements CDProtocol, EDProtocol {

    private static final String PAR_TREE_PROT = "tree_protocol";
    private final int tree;
    private final String prefix;


//--------------------------------------------------------------------------
// Initialization
//--------------------------------------------------------------------------

    public TreeProtocol(String prefix) {
        tree = Configuration.getPid(prefix + "." + PAR_TREE_PROT);
        this.prefix = prefix;
    }

//--------------------------------------------------------------------------
// methods
//--------------------------------------------------------------------------

    /**
     * This is the standard method the define periodic activity.
     * The frequency of execution of this method is defined by a
     * {@link peersim.edsim.CDScheduler} component in the configuration.
     */
    public void nextCycle(Node node, int pid) {
        doDatabaseMethod(node, pid);
    }

    /**
     * Every client attempts to do something.
     */
    private void doDatabaseMethod(Node node, int pid) {
        for (Client client : clients) {
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                //System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }



            // If is Local Read
            if (eventIsRead(operation)) {
                ReadMessage readMessage = new ReadMessage(this.getNodeId(), client.getId(), operation.getKey());
                sendMessage(node, node, readMessage, pid);
                continue;
            }

            // If is local update
            if (eventIsUpdate(operation)) {
                LocalUpdate localUpdate = new LocalUpdate(client.getId(), operation.getKey());
                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }

        this.checkIfCanAcceptMigratedClients();

        processTreeNodeCycle(node, pid);

    }

    private void processTreeNodeCycle(Node node, int pid){

        StateTreeProtocolInstance treeNode = (StateTreeProtocolInstance) node.getProtocol(tree);

        //send data in parallel
        Message message = treeNode.getParallelMessage();
        while (message != null){
            //TODO send message remoteUpdate TARAS
            Node destinationNode = Network.get((int)message.getNodeDestinationID());
            message.setNodeOriginID(node.getID());
            sendMessage(node, destinationNode, message, pid);
            message = treeNode.getParallelMessage();

        }

        //send metadata in fifo
        message = treeNode.getFIFOMessage();
        while (message != null){
            //TODO send message to brokers TARAS tem que ser em canais diferentes
            Node destinationNode = Network.get((int)message.getNodeDestinationID());
            message.setNodeOriginID(node.getID());
            sendMessage(node, destinationNode, message, pid);
            message = treeNode.getFIFOMessage();

        }
    }


    private Node getMigrationDatacenter(int key,
                                        StateTreeProtocol originalDC) {

        // Get datacenters that have the key
        Set<Node> interestedNodes = getInterestedDatacenters(key);

        // Then select the datacenter that has the lowest latency to the client
        return getLowestLatencyDatacenter(originalDC, interestedNodes);
    }

    private Set<Node> getInterestedDatacenters(int key) {
        Set<Node> interestedNodes = new HashSet<>();
        // First get which datacenters replicate the data
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocol datacenter = (StateTreeProtocol) node.getProtocol(tree);

            if (datacenter.isInterested(key)) {
                interestedNodes.add(node);
            }
        }
        return interestedNodes;
    }

    private Node getLowestLatencyDatacenter(StateTreeProtocol originalDC, Set<Node> interestedNodes) {
        int lowestLatency = Integer.MAX_VALUE;
        Node bestNode = null;
        for (Node interestedNode : interestedNodes) {
            int nodeLatency = PointToPointTransport
                    .staticGetLatency(originalDC.getNodeId(), interestedNode.getID());
            if (nodeLatency < lowestLatency) {
                bestNode = interestedNode;
            }
        }
        return bestNode;
    }


    private void sendMessage(Node src, Node dst, Object msg, int pid) {
        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

//--------------------------------------------------------------------------

    /**
     * This is the standard method to define to process incoming messages.
     */
    public void processEvent(Node node, int pid, Object event) {



    }

    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------


    public Object clone() {
        return new TreeProtocol(prefix);
    }

    private boolean eventIsRead(Operation operation) {
        return operation.getType() == Operation.Type.READ;
    }

    private boolean eventIsUpdate(Operation operation) {
        return operation.getType() == Operation.Type.UPDATE;
    }



    /*
    private void debugCheckIfNodeIsPartitioned(Node bestNode) {
        Map<Long, Integer> longIntegerMap = PointToPointTransport.partitionTable.get(bestNode.getID());
        for (Long dstNode : longIntegerMap.keySet()) {
            if (longIntegerMap.get(dstNode) > CommonState.getTime()) {
                System.out.println("MIGRATING TO PARTITIONED NODE!" + longIntegerMap.get(dstNode) + " | " + CommonState.getTime());
            }
        }
    }
    */


//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

    class ReadMessage {

        final long senderDC;
        final int clientId;
        final int key;
        final long timestamp;

        public ReadMessage(long senderDC, int clientId, int key) {
            this.senderDC = senderDC;
            this.clientId = clientId;
            this.key = key;
            timestamp = CommonState.getTime();
        }
    }

    class LocalUpdate {

        final int clientId;
        final int key;
        final long timestamp;

        public LocalUpdate(int clientId, int key) {
            this.clientId = clientId;
            this.key = key;
            timestamp = CommonState.getTime();
        }
    }

    class RemoteUpdateMessage {

        final long senderDC;
        final int clientId;
        // final EventUID event;
        final int key;
        final Map<Integer, Integer> context;
        final Integer version;
        final long timestamp;

        RemoteUpdateMessage(long sender,
                            int clientId,
                            int key,
                            // EventUID event,
                            Map<Integer, Integer> context,
                            Integer version) {
            this.senderDC = sender;
            this.clientId = clientId;
            this.key = key;
            //this.event = event;
            this.context = context;
            this.version = version;
            timestamp = CommonState.getTime();
        }
    }
}


