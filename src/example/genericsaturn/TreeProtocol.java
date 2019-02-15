package example.genericsaturn;

import example.genericsaturn.datatypes.EventUID;
import example.genericsaturn.datatypes.Operation;
import example.genericsaturn.datatypes.UpdateOperation;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static example.genericsaturn.TypeProtocol.Type;

public class TreeProtocol extends StateTreeProtocolInstance
        implements CDProtocol, EDProtocol {

    private final int typePid;
    private final int tree;
    private static final String PAR_TYPE_PROT = "type_protocol";
    private static final String PAR_TREE_PROT = "tree_protocol";
    private final String prefix;


//--------------------------------------------------------------------------
// Initialization
//--------------------------------------------------------------------------

    public TreeProtocol(String prefix) {
        super(prefix);
        this.prefix = prefix;
        tree = Configuration.getPid(prefix + "." + PAR_TREE_PROT);
        typePid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
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
        //System.out.println("About to clean pendings, node "+node.getID()+", epoch"+getEpoch());
        processPendingEpoch(getEpoch());
        cleanPendingQueue(getEpoch());
        newEpoch();
        Linkable linkable = (Linkable) node.getProtocol(FastConfig.getLinkable(pid));
        //System.out.println("Node "+node.getIndex()+"(ID: "+node.getID()+" and degree: "+linkable.degree()+"): I am sending and event");
        TypeProtocol sendertype = (TypeProtocol) node.getProtocol(typePid);
        if (sendertype.getType() == Type.DATACENTER) {
            doDatabaseMethod(node, pid, linkable);
        } else {
            doBrokerMethod(node, pid, linkable);
        }
    }

    /**
     * Every client attempts to do something.
     */
    private void doDatabaseMethod(Node node, int pid, Linkable linkable) {
        Iterator<Client> it = clients.iterator();
        while (it.hasNext()) {
            Client client = it.next();
            Operation operation = client.nextOperation();
            EventUID event = new EventUID(operation, client.timestamp(), getEpoch(), node.getID(), 0);
            analyzeEvent(node, event);

            // If there isn't a remote operation, continue
            if (event.getOperation().getType() == Operation.Type.READ) {
                continue;
            }

            // If it's a remote read, migrate the client
            if (event.getOperation().getType() == Operation.Type.REMOTE_READ) {
                migrateClientStart(client, event, node, linkable, pid);
                it.remove();
            }

            handleRemoteOperation(node, pid, linkable, operation, event);
        }
    }

    private void handleRemoteOperation(Node node, int pid, Linkable linkable, Operation operation, EventUID event) {
        for (int i = 0; i < linkable.degree(); i++) {
            Node peern = linkable.getNeighbor(i);
            EventUID eventToSend = event.clone();
            eventToSend.setDst(peern.getID());

            // XXX quick and dirty handling of failures
            // (message would be lost anyway, we save time)

            if (!peern.isUp()) {
                continue;
            }

            TypeProtocol ntype = (TypeProtocol) peern.getProtocol(typePid);
            if (ntype.getType() == Type.DATACENTER) {
                if (isInterested(peern.getID(), operation.getKey())) {
                    sendMessage(node, peern, new DataMessage(eventToSend, "data", getEpoch()), pid);
                }
            } else if (ntype.getType() == Type.BROKER) {
                sendMessage(node, peern, new MetadataMessage(eventToSend, getEpoch(), node.getID()), pid);
            }
        }
    }

    private void analyzeEvent(Node node, EventUID event) {
        if (event.getOperation().getType() == Operation.Type.READ) {
            long key = event.getOperation().getKey();

            if (isInterested(node.getID(), key)) {
                this.incrementLocalReads();
            } else {
                //remote read
                this.incrementRemoteReads();
                event.getOperation().setType(Operation.Type.REMOTE_READ);
            }
        } else if (event.getOperation().getType() == Operation.Type.UPDATE) {
            incrementUpdates();
            UpdateOperation update = (UpdateOperation) event.getOperation();
            addFullMetadata(update.getMetadataFull());
            addPartialMetadata(update.getMetadataPartial());
        }
    }

    private void doBrokerMethod(Node node, int pid, Linkable linkable) {
        for (int i = 0; i < linkable.degree(); i++) {
            Node peerNode = linkable.getNeighbor(i);
            TypeProtocol peerType = (TypeProtocol) peerNode.getProtocol(typePid);

            if (!peerNode.isUp()) {
                return;
            }

            List<EventUID> queueToSend = queue.get(peerNode.getID());
            if (queueToSend != null) {
                QueueMessage msg = new QueueMessage(
                        new ArrayList<>(queueToSend),
                        peerType.getType(),
                        getEpoch(),
                        node.getID());
                sendMessage(node, peerNode, msg, pid);
                this.cleanQueue(peerNode.getID());
            }
        }
    }

    private void migrateClientStart(Client client,
                                    EventUID event,
                                    Node originalDC,
                                    Linkable linkable,
                                    int pid) {

        Set<Node> interestedNodes = new HashSet<>();
        // First get which datacenters replicate the data
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);

            // Can't migrate to a broker
            if (((TypeProtocol) node.getProtocol(typePid)).getType() == Type.BROKER) {
                continue;
            }

            if (isInterested(node.getID(), event.getOperation().getKey())) {
                interestedNodes.add(node);
            }
        }

        // Then select the datacenter that has the lowest latency to the client
        TypeProtocol ntype = (TypeProtocol) originalDC.getProtocol(typePid);
        int lowestLatency = Integer.MAX_VALUE;
        Node bestNode = null;
        for (Node interestedNode : interestedNodes) {
            int nodeLatency = ntype.getLatency(interestedNode.getID());
            if (nodeLatency < lowestLatency) {
                bestNode = interestedNode;
            }
        }

        // Check if node is under partition
        /*

        Map<Long, Integer> longIntegerMap = PointToPointTransport.partitionTable.get(bestNode.getID());
        for (Long dstNode : longIntegerMap.keySet()) {
            if (longIntegerMap.get(dstNode) > CommonState.getTime()) {
                System.out.println("MIGRATING TO PARTITIONED NODE!" + longIntegerMap.get(dstNode) + " | " + CommonState.getTime());
            }
        }
        */

        // Generate Migration Label;
             EventUID migrationLabel = event.clone();
        migrationLabel.setMigration(true, bestNode.getID());

        // Send client to target
        TreeProtocol targetDCProtocol = (TreeProtocol) bestNode.getProtocol(tree);
        targetDCProtocol.migrateClientQueue(client, migrationLabel);

        sentMigrations++;

        // Send label to broker network
        sendToBrokerNetwork(originalDC, migrationLabel, linkable, pid);
    }

    private void sendToBrokerNetwork(Node originalDC, EventUID migrationLabel, Linkable linkable, int pid) {
        for (int i = 0; i < linkable.degree(); i++) {
            Node peer = linkable.getNeighbor(i);
            TypeProtocol ntype = (TypeProtocol) peer.getProtocol(typePid);
            if (!peer.isUp()) {
                return;
            }

            if (ntype.getType() == Type.BROKER) {
                sendMessage(originalDC, peer, new MigrationMessage(migrationLabel, originalDC.getID(), epoch), pid);
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

        /* ************** DATACENTERS ****************** */
        if (event instanceof DataMessage) {
            DataMessage msg = (DataMessage) event;
            this.addData(msg.event, msg.data);
        }

        if (event instanceof MetadataMessage) {
            MetadataMessage msg = (MetadataMessage) event;
            if (msg.epoch == this.getEpoch()) {
                this.addToQueue(msg.event, msg.senderId);
            } else {
                this.addToPendingQueue(msg.event, msg.epoch, msg.senderId);
            }
        }

        /* ************** BROKERS ****************** */
        if (event instanceof MigrationMessage) {
            MigrationMessage msg = (MigrationMessage) event;

            List<EventUID> vec = new ArrayList<>();
            vec.add(msg.event);
            if (msg.epoch == this.getEpoch()) {
                this.addQueueToQueue(vec, msg.senderId);
            } else {
                this.addQueueToPendingQueue(vec, msg.epoch, msg.senderId);
            }
        }

        if (event instanceof QueueMessage) {
            QueueMessage msg = (QueueMessage) event;

            if (msg.type == Type.DATACENTER) {
                this.processQueue(msg.queue, node.getID());
            } else {
                if (msg.epoch == this.getEpoch()) {
                    this.addQueueToQueue(msg.queue, msg.senderId);
                } else {
                    this.addQueueToPendingQueue(msg.queue, msg.epoch, msg.senderId);
                }
            }
        }
    }

    @Override
    public Object clone() {
        return new TreeProtocol(prefix);
    }

//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

    class DataMessage {

        final EventUID event;
        final Object data;
        final int epoch;

        DataMessage(EventUID event, Object data, int epoch) {
            this.event = event;
            this.data = data;
            this.epoch = epoch;
        }
    }

    class MetadataMessage {

        final EventUID event;
        final int epoch;
        final long senderId;

        MetadataMessage(EventUID event, int epoch, long senderId) {
            this.event = event;
            this.epoch = epoch;
            this.senderId = senderId;
        }

    }

    class MigrationMessage {

        final EventUID event;
        final long senderId;
        final int epoch;

        MigrationMessage(EventUID event, long senderId, int epoch) {
            this.event = event;
            this.senderId = senderId;
            this.epoch = epoch;
        }
    }


    class QueueMessage {

        final List<EventUID> queue;
        final Type type;
        final int epoch;

        final long senderId;

        QueueMessage(List<EventUID> queue, Type type, int epoch, long senderId) {
            this.queue = queue;
            this.type = type;
            this.epoch = epoch;
            this.senderId = senderId;
        }
    }
}

