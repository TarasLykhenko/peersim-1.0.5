package example.saturn;

import example.common.MigrationMessage;
import example.common.PointToPointTransport;
import example.common.datatypes.Operation;
import example.saturn.datatypes.UpdateOperation;
import example.saturn.datatypes.message.types.*;
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
    static public int tree;
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
        StateTreeProtocolInstance treeNode = (StateTreeProtocolInstance) node.getProtocol(tree);
        treeNode.updateUsedBandwidth();
        processClientsNodeCycle(node, pid);
        processTreeNodeCycle(node, pid);

    }

    /**
     * Every client attempts to do something.
     */
    private void processClientsNodeCycle(Node node, int pid) {

        for (Client client : clients) {
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                //System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }


            // If is Local Read
            if (eventIsRead(operation)) {
                ReadMessage readMessage = new ReadMessage(operation.getKey(), client.getId());
                readMessage.setNodeOriginID(this.getNodeId());
                sendMessage(node, node, readMessage, pid);
                continue;
            }

            // If is local update
            if (eventIsUpdate(operation)) {
                UpdateOperation updateOperation = (UpdateOperation) operation;
                LocalUpdateMessage localUpdate = new LocalUpdateMessage(operation.getKey(), client.getId(), updateOperation.getValue());
                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }

      //  this.checkIfCanAcceptMigratedClients();

    }

    private void processTreeNodeCycle(Node node, int pid){

        StateTreeProtocolInstance treeNode = (StateTreeProtocolInstance) node.getProtocol(tree);
        treeNode.updateUsedBandwidth();
        //send data in parallel
        Message message;
        while ((message = treeNode.getParallelMessage()) != null){
            Node destinationNode = Network.get((int)message.getNodeDestinationID());
            message.setNodeOriginID(node.getID());
            sendMessage(node, destinationNode, message, pid);

        }

        //send metadata in fifo
        while ((message = treeNode.getFIFOMessage()) != null){
            Node destinationNode = Network.get((int)message.getNodeDestinationID());
            message.setNodeOriginID(node.getID());
            sendMessage(node, destinationNode, message, pid);

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

        StateTreeProtocolInstance treeNode = (StateTreeProtocolInstance) node.getProtocol(tree);

        if (event instanceof LocalUpdateMessage) {
            LocalUpdateMessage localUpdateMessage = (LocalUpdateMessage) event;
            long value = treeNode.localUpdateMessage(localUpdateMessage);
            Client client = idToClient.get(localUpdateMessage.getClientId());
            client.receiveUpdateResult(localUpdateMessage.getKey(),value);

        }

        else if(event instanceof ReadMessage){
            ReadMessage readMessage = (ReadMessage) event;
            long value = treeNode.readMessage( readMessage);
            Client client = idToClient.get(readMessage.getClientId());
            client.receiveReadResult(readMessage.getKey(),value);
        }

        else if(event instanceof MetadataMessage){
            treeNode.metadataMessage( (MetadataMessage) event);
        }

        else if(event instanceof RemoteUpdateMessage){
            treeNode.remoteUpdateMessage( (RemoteUpdateMessage) event);
        }
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





//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

}


