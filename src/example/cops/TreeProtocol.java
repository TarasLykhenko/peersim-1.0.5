package example.cops;

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
        super(prefix);
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
        StateTreeProtocol datacenter = (StateTreeProtocol) node.getProtocol(tree);
        Iterator<Client> it = clients.iterator();

        while (it.hasNext()) {
            Client client = it.next();
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }
            EventUID event = new EventUID(operation, CommonState.getTime(), datacenter.getNodeId(), 0);
            analyzeEvent(datacenter, event);

            // If there isn't a remote operation, continue
            // TODO Important! There's no latency in local operations.
            if (event.getOperation().getType() == Operation.Type.READ) {
                int version = copsGet(operation.getKey());
                client.receiveReadResult(operation.getKey(), version);
                continue;
            }

            // If it's a remote read, migrate the client
            if (event.getOperation().getType() == Operation.Type.REMOTE_READ) {
                migrateClientStart(client, event, datacenter);
                System.out.println("Client " + client.getId() + " has migrated!");
                it.remove();
            }

            if (event.getOperation().getType() == Operation.Type.UPDATE) {
                // If the client's DC is interested, apply the update locally
                if (datacenter.isInterested(event.getOperation().getKey())) {
                    Integer newVersion = datacenter.copsPut(event.getOperation().getKey());
                    client.receiveUpdateResult(event.getOperation().getKey(), newVersion);

                    for (int i = 0; i < linkable.degree(); i++) {
                        Node peer = linkable.getNeighbor(i);
                        EventUID eventToSend = new EventUID(event);
                        eventToSend.setDst(peer.getID());

                        // XXX quick and dirty handling of failures
                        // (message would be lost anyway, we save time)

                        if (!peer.isUp()) {
                            return;
                        }

                        StateTreeProtocol peerDatacenter = (StateTreeProtocol) peer.getProtocol(tree);
                        if (peerDatacenter.isInterested(operation.getKey())) {
                            DataMessage msg = new DataMessage(eventToSend,
                                    client.getCopyOfContext(),
                                    newVersion,
                                    CommonState.getTime());
                            sendMessage(node, peer, msg, pid);
                        }
                    }
                } else {
                    System.out.println("TODO for client " + client.getId());
                    // TODO force migrate I guess
                }
            }

        }
    }

    // TODO - Pode fazer remote updates antes de migrar!
    private void analyzeEvent(StateTreeProtocol datacenter, EventUID event) {
        if (event.getOperation().getType() == Operation.Type.READ) {
            int key = event.getOperation().getKey();

            if (datacenter.isInterested(key)) {
                this.incrementLocalReads();
            } else {
                //remote read
                this.incrementRemoteReads();
                event.getOperation().setType(Operation.Type.REMOTE_READ);
            }
        } else if (event.getOperation().getType() == Operation.Type.UPDATE) {
            incrementUpdates();
        }
    }

    private void migrateClientStart(Client client,
                                    EventUID event,
                                    StateTreeProtocol originalDC) {

        Set<Node> interestedNodes = new HashSet<>();
        // First get which datacenters replicate the data
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocol datacenter = (StateTreeProtocol) node.getProtocol(tree);

            if (datacenter.isInterested(event.getOperation().getKey())) {
                interestedNodes.add(node);
            }
        }

        // Then select the datacenter that has the lowest latency to the client
        int lowestLatency = Integer.MAX_VALUE;
        Node bestNode = null;
        for (Node interestedNode : interestedNodes) {

            int nodeLatency = PointToPointTransport
                    .staticGetLatency(originalDC.getNodeId(), interestedNode.getID());
            if (nodeLatency < lowestLatency) {
                bestNode = interestedNode;
            }
        }
        if (bestNode == null) {
            System.out.println("TRUMP.SAD()!");
        }

        // Check if node is under partition
        // debugCheckIfNodeIsPartitioned(bestNode);


        // Send client to target
        TreeProtocol targetDCProtocol = (TreeProtocol) bestNode.getProtocol(tree);
        targetDCProtocol.migrateClientQueue(client, client.getCopyOfContext());

        sentMigrations++;
    }

    private void debugCheckIfNodeIsPartitioned(Node bestNode) {
        Map<Long, Integer> longIntegerMap = PointToPointTransport.partitionTable.get(bestNode.getID());
        for (Long dstNode : longIntegerMap.keySet()) {
            if (longIntegerMap.get(dstNode) > CommonState.getTime()) {
                System.out.println("MIGRATING TO PARTITIONED NODE!" + longIntegerMap.get(dstNode) + " | " + CommonState.getTime());
            }
        }
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

    public Object clone() {
        return new TreeProtocol(prefix);
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


