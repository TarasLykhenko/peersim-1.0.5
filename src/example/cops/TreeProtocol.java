package example.cops;

import example.common.PointToPointTransport;
import example.cops.datatypes.EventUID;
import example.cops.datatypes.Operation;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Linkable;
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
        Linkable linkable = (Linkable) node.getProtocol(FastConfig.getLinkable(pid));
        doDatabaseMethod(node, pid, linkable);
    }

    /**
     * Every client attempts to do something.
     */
    private void doDatabaseMethod(Node node, int pid, Linkable linkable) {
        StateTreeProtocolInstance datacenter = (StateTreeProtocolInstance) node.getProtocol(tree);

        for (Client client : clients) {
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                //System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }
            EventUID event = new EventUID(operation, CommonState.getTime(), datacenter.getNodeId(), 0);

            if (!datacenter.isInterested(operation.getKey())) {
                operation.setType(Operation.Type.REMOTE_READ);
            }


            // If is Local Read
            if (eventIsRead(event)) {
                ReadMessage readMessage = new ReadMessage(this.getNodeId(), client.getId(), operation.getKey());
                sendMessage(node, node, readMessage, pid);
                continue;
            }

            // If it's a remote read, migrate the client
            // Note: If it's an update but the DC doesn't have the object, it becomes a remote read
            if (event.getOperation().getType() == Operation.Type.REMOTE_READ) {
                MigrationMessage msg = new MigrationMessage(datacenter.getNodeId(), client.getId());
                Node targetDC = getMigrationDatacenter(event, datacenter);

                sendMessage(node, targetDC, msg, pid);
                sentMigrations++;
                continue;
            }

            // If is local update
            if (eventIsUpdate(event) && datacenter.isInterested(event.getOperation().getKey())) {
                LocalUpdate localUpdate = new LocalUpdate(client.getId(), operation.getKey());
                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }
        datacenter.checkIfCanAcceptMigratedClients();
    }

    private Node getMigrationDatacenter(EventUID event,
                                        StateTreeProtocol originalDC) {

        Set<Node> interestedNodes = getInterestedDatacenters(event);

        // Then select the datacenter that has the lowest latency to the client
        return getLowestLatencyDatacenter(originalDC, interestedNodes);

        // Send client to target
        // TreeProtocol targetDCProtocol = (TreeProtocol) bestNode.getProtocol(tree);
        // targetDCProtocol.migrateClientQueue(client, client.getCopyOfContext());

        // sentMigrations++;
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

    private Set<Node> getInterestedDatacenters(EventUID event) {
        return getInterestedDatacenters(event.getOperation().getKey());
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

        if (event instanceof LocalUpdate) {
            LocalUpdate localUpdate = (LocalUpdate) event;

            int newVersion = this.copsPut(localUpdate.key);
            Client client = idToClient.get(localUpdate.clientId);

            Set<Node> interestedDatacenters = getInterestedDatacenters(localUpdate.key);
            // Remove self from remote update list
            interestedDatacenters.remove(Network.get(Math.toIntExact(this.getNodeId())));
            for (Node interestedNode : interestedDatacenters) {

                RemoteUpdateMessage remoteMsg = new RemoteUpdateMessage(
                        this.getNodeId(),
                        client.getId(),
                        localUpdate.key,
                        client.getCopyOfContext(),
                        newVersion);

                sendMessage(node, interestedNode, remoteMsg, pid);
            }

            client.receiveUpdateResult(localUpdate.key, newVersion);
        }

        if (event instanceof RemoteUpdateMessage) {
            RemoteUpdateMessage msg = (RemoteUpdateMessage) event;

            if (msg.senderDC == nodeId) {
                throw new RuntimeException("Remote LOCAL update?");
            }

            // Remote update
            this.copsPutRemote(msg.key,
                    msg.context,
                    msg.version);

        }

        if (event instanceof MigrationMessage) {
            MigrationMessage msg = (MigrationMessage) event;
            StateTreeProtocolInstance originalDC = (StateTreeProtocolInstance)
                    Network.get(Math.toIntExact(msg.senderDC)).getProtocol(tree);

            Client client = originalDC.idToClient.get(msg.clientId);
            // Remove client from original DC
            originalDC.clients.remove(client);
            originalDC.idToClient.remove(msg.clientId);

            // Put client on Queue
            this.migrateClientQueue(client, client.getCopyOfContext());
        }

        if (event instanceof ReadMessage) {
            ReadMessage msg = (ReadMessage) event;
            if (msg.senderDC != nodeId) {
                throw new RuntimeException("Reads must ALWAYS be local.");
            }
            int keyVersion = copsGet(msg.key);
            this.idToClient.get(msg.clientId).receiveReadResult(msg.key, keyVersion);
        }
    }

    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------


    public Object clone() {
        return new TreeProtocol(prefix);
    }

    private boolean eventIsRead(EventUID event) {
        return event.getOperation().getType() == Operation.Type.READ;
    }

    private boolean eventIsUpdate(EventUID event) {
        return event.getOperation().getType() == Operation.Type.UPDATE;
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

    class MigrationMessage {
        final long senderDC;
        final int clientId;
        final long timestamp;

        MigrationMessage(long senderDC, int clientId) {
            this.senderDC = senderDC;
            this.clientId = clientId;
            timestamp = CommonState.getTime();
        }
    }

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


