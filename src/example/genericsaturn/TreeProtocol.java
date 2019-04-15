package example.genericsaturn;

import example.common.datatypes.Operation;
import example.genericsaturn.datatypes.EventUID;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.ArrayList;
import java.util.List;

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
        for (Client client : clients) {
            Operation operation = client.nextOperation();

            if (operation == null) {
                continue;
            }
            // System.out.println("Client " + client.getId() + " wants " + operation.getKey());
            EventUID event = new EventUID(operation, client.timestamp(), getEpoch(), node.getID(), 0);

            // If is remote operation, migrate
            if (operation.getDatacenter() != nodeId) {
            //    System.out.println("Starting migration");
                migrateClientStart(client, event, node, linkable, pid);
                continue;
            }

            // If is Local Read
            if (eventIsRead(event)) {
                ReadMessage readMessage = new ReadMessage(this.getNodeId(), client.getId(), operation.getKey());
                sendMessage(node, node, readMessage, pid);
                continue;
            }

            // If is local update
            if (eventIsUpdate(event)) {
                LocalUpdate localUpdate = new LocalUpdate(client.getId(), operation.getKey(), event);
                sendMessage(node, node, localUpdate, pid);
                // handleRemoteOperation(node, pid, linkable, operation, event);
            } else {
                System.out.println("Unknown scenario!");
            }
        }
    }

    private void handleRemoteOperation(Node node, int pid, EventUID event) {
        Linkable linkable = (Linkable) node.getProtocol(FastConfig.getLinkable(pid));
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
                StateTreeProtocol peerDatacenter = (StateTreeProtocol) peern.getProtocol(tree);
                if (peerDatacenter.isInterested(event.getOperation().getKey())) {
                    // Spread data
                    sendMessage(node, peern, new DataMessage(eventToSend, "data", getEpoch()), pid);
                }
            } else if (ntype.getType() == Type.BROKER) {
                // Spread metadata
                sendMessage(node, peern, new MetadataMessage(eventToSend, getEpoch(), node.getID()), pid);
            }
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
            if (queueToSend != null && !queueToSend.isEmpty()) {
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

        Node bestNode = Network.get((int) event.getOperation().getDatacenter());

        // Generate Migration Label;
        EventUID migrationLabel = event.clone();
        migrationLabel.setMigration(true, bestNode.getID());

        client.migrationStart();
        MigrationMessage migrationMessage = new MigrationMessage(nodeId, client.getId(), migrationLabel);
        sendMessage(originalDC, bestNode, migrationMessage, pid);
        sentMigrations++;

        // Send label to broker network
        sendToBrokerNetwork(originalDC, migrationLabel, pid);
    }

    private void sendToBrokerNetwork(Node originalDC, EventUID migrationLabel, int pid) {
        Linkable linkable = (Linkable) originalDC.getProtocol(FastConfig.getLinkable(pid));
        for (int i = 0; i < linkable.degree(); i++) {
            Node peer = linkable.getNeighbor(i);
            TypeProtocol ntype = (TypeProtocol) peer.getProtocol(typePid);
            if (!peer.isUp()) {
                return;
            }

            if (ntype.getType() == Type.BROKER) {
                sendMessage(originalDC, peer, new BrokerMigrationMessage(migrationLabel, originalDC.getID(), epoch), pid);
            }
        }
    }

    private void sendMessage(Node src, Node dst, Object msg, int pid) {
        ((Transport) src.getProtocol(FastConfig.getTransport(pid)))
                .send(src, dst, msg, pid);
    }

    private boolean eventIsRead(EventUID event) {
        return event.getOperation().getType() == Operation.Type.READ;
    }

    private boolean eventIsUpdate(EventUID event) {
        return event.getOperation().getType() == Operation.Type.UPDATE;
    }


//--------------------------------------------------------------------------

    /**
     * This is the standard method to define to process incoming messages.
     */
    public void processEvent(Node node, int pid, Object event) {

        /* ************** DATACENTERS ****************** */

        if (event instanceof ReadMessage) {
            ReadMessage msg = (ReadMessage) event;
            if (msg.senderDC != nodeId) {
                throw new RuntimeException("Reads must ALWAYS be local.");
            }

            this.idToClient.get(msg.clientId).receiveReadResult(msg.key, null);
        }

        if (event instanceof LocalUpdate) {
            LocalUpdate localUpdate = (LocalUpdate) event;
            Client client = idToClient.get(localUpdate.clientId);
            client.receiveUpdateResult(localUpdate.key, null);
            handleRemoteOperation(node, pid, localUpdate.event);
        }

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

        if (event instanceof MigrationMessage) {
            MigrationMessage msg = (MigrationMessage) event;

            StateTreeProtocolInstance originalDC = (StateTreeProtocolInstance)
                    Network.get(Math.toIntExact(msg.senderDC)).getProtocol(tree);

            Client client = originalDC.idToClient.get(msg.clientId);
            // Remove client from original DC
            originalDC.clients.remove(client);
            originalDC.idToClient.remove(msg.clientId);

            this.migrateClientQueue(client, msg.eventUID);
        }

        /* ************** BROKERS ****************** */
        if (event instanceof BrokerMigrationMessage) {
            BrokerMigrationMessage msg = (BrokerMigrationMessage) event;

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
        final EventUID event;
        final long timestamp;

        public LocalUpdate(int clientId, int key, EventUID event) {
            this.clientId = clientId;
            this.key = key;
            this.event = event;
            timestamp = CommonState.getTime();
        }
    }

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

    class BrokerMigrationMessage {

        final EventUID event;
        final long senderId;
        final int epoch;

        BrokerMigrationMessage(EventUID event, long senderId, int epoch) {
            this.event = event;
            this.senderId = senderId;
            this.epoch = epoch;
        }
    }

    class MigrationMessage {
        final long senderDC;
        final int clientId;
        final long timestamp;
        final EventUID eventUID;

        public MigrationMessage(long senderDC, int clientId, EventUID event) {
            this.senderDC = senderDC;
            this.clientId = clientId;
            this.timestamp = CommonState.getTime();
            this.eventUID = event;
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

