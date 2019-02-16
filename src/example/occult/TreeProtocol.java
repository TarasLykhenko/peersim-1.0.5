package example.occult;

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
import java.util.Iterator;
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
        Iterator<Client> it = clients.iterator();

        while (it.hasNext()) {
            Client client = it.next();
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                //System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }
            EventUID event = new EventUID(operation, CommonState.getTime(), datacenter.getNodeId(), 0);

            //
            if (operation.getType() == Operation.Type.UPDATE) {

            }

            if (!datacenter.isInterested(operation.getKey())) {
                operation.setType(Operation.Type.REMOTE_READ);
            }


            // If there isn't a remote operation, continue
            if (eventIsRead(event)) {
                int version = copsGet(operation.getKey());
                client.receiveReadResult(operation.getKey(), version);
                continue;
            }

            // If it's a remote read, migrate the client
            // Note: If it's an update but the DC doesn't have the object, it becomes a remote read
            if (event.getOperation().getType() == Operation.Type.REMOTE_READ) {
                migrateClientStart(client, event, datacenter);
                it.remove();
                continue;
            }

            // If the client's DC is interested, apply the update locally
            if (eventIsUpdate(event) && datacenter.isInterested(event.getOperation().getKey())) {
                Integer newVersion = datacenter.copsPut(event.getOperation().getKey(), CommonState.getTime());

                Map<Integer, Integer> clientContext = client.getCopyOfContext();
                client.receiveUpdateResult(event.getOperation().getKey(), newVersion);

                for (int i = 0; i < linkable.degree(); i++) {
                    Node peer = linkable.getNeighbor(i);
                    StateTreeProtocol peerDatacenter = (StateTreeProtocol) peer.getProtocol(tree);

                    if (peerDatacenter.isInterested(operation.getKey())) {
                        EventUID eventToSend = new EventUID(event);
                        eventToSend.setDst(peer.getID());

                        DataMessage msg = new DataMessage(eventToSend,
                                clientContext,
                                newVersion,
                                CommonState.getTime());
                        sendMessage(node, peer, msg, pid);
                    }
                }
            } else {
                System.out.println("Unknown scenario!");
            }
        }
        datacenter.checkIfCanAcceptMigratedClients();
    }

    private void migrateClientStart(Client client,
                                    EventUID event,
                                    StateTreeProtocol originalDC) {

        Set<Node> interestedNodes = getInterestedDatacenters(event);

        // Then select the datacenter that has the lowest latency to the client
        Node bestNode = getLowestLatencyDatacenter(originalDC, interestedNodes);

        // Send client to target
        TreeProtocol targetDCProtocol = (TreeProtocol) bestNode.getProtocol(tree);
        targetDCProtocol.migrateClientQueue(client, client.getCopyOfContext());

        sentMigrations++;
    }

    private Set<Node> getInterestedDatacenters(EventUID event) {
        Set<Node> interestedNodes = new HashSet<>();
        // First get which datacenters replicate the data
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocol datacenter = (StateTreeProtocol) node.getProtocol(tree);

            if (datacenter.isInterested(event.getOperation().getKey())) {
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

        if (event instanceof DataMessage) {
            DataMessage msg = (DataMessage) event;
            this.copsPutRemote(msg.event.getOperation().getKey(),
                    msg.context,
                    msg.version);
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

    private void debugCheckIfNodeIsPartitioned(Node bestNode) {
        Map<Long, Integer> longIntegerMap = PointToPointTransport.partitionTable.get(bestNode.getID());
        for (Long dstNode : longIntegerMap.keySet()) {
            if (longIntegerMap.get(dstNode) > CommonState.getTime()) {
                System.out.println("MIGRATING TO PARTITIONED NODE!" + longIntegerMap.get(dstNode) + " | " + CommonState.getTime());
            }
        }
    }


//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

    class DataMessage {

        final EventUID event;
        final Map<Integer, Integer> context;
        final Integer version;
        final long timestamp;

        DataMessage(EventUID event, Map<Integer, Integer> context, Integer version, long timestamp) {
            this.event = event;
            this.context = context;
            this.version = version;
            this.timestamp = timestamp;
        }
    }
}


