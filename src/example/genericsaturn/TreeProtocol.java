package example.genericsaturn;

import example.genericsaturn.datatypes.EventUID;
import example.genericsaturn.datatypes.Operation;
import example.genericsaturn.datatypes.ReadOperation;
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
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import static example.genericsaturn.TypeProtocol.Type;

public class TreeProtocol extends StateTreeProtocolInstance
        implements CDProtocol, EDProtocol {

    private final int typePid;
    private final int tree = 2; //This is very hardcoded :/
    private static final String PAR_TYPE_PROT = "type_protocol";
    private static final String PAR_TREE_PROT = "tree_protocol";


//--------------------------------------------------------------------------
// Initialization
//--------------------------------------------------------------------------

    public TreeProtocol(String prefix) {
        super(prefix);
        //tree = Configuration.getPid(prefix + "." + PAR_TREE_PROT);
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
            EventUID event = new EventUID(operation, client.timestamp(), getEpoch(),0, node.getID(), 0);
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

            for (int i = 0; i < linkable.degree(); i++) {
                Node peern = linkable.getNeighbor(i);
                EventUID eventToSend = event.clone();
                eventToSend.setLatency(((Transport) node.getProtocol(FastConfig.getTransport(pid))).getLatency(node, peern));
                eventToSend.setDst(peern.getID());
                // XXX quick and dirty handling of failures
                // (message would be lost anyway, we save time)

                if (!peern.isUp()) {
                    return;
                }

                TypeProtocol ntype = (TypeProtocol) peern.getProtocol(typePid);
                //if ((getEpoch() % ntype.getFrequency(node.getID())) == 0 ){
                switch (ntype.getType()) {
                    case DATACENTER:
                        if (isInterested(peern.getID(), operation.getKey())) {
                            //System.out.println("Sending event "+eventToSend.getKey()+","+eventToSend.getTimestamp()+" (epoch "+getEpoch()+") from: "+eventToSend.getSrc()+" to: "+eventToSend.getDst()+" with latency: "+eventToSend.getLatency());
                            sendMessage(node, peern, new DataMessage(eventToSend, "data", getEpoch()), pid);
                        }
                        break;
                    case BROKER:
                        sendMessage(node, peern, new MetadataMessage(eventToSend, getEpoch(), node.getID()), pid);

                }
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
        //    System.out.println("Doing broker!");
        for (int i = 0; i < linkable.degree(); i++) {
            Node peern = linkable.getNeighbor(i);
            TypeProtocol ntype = (TypeProtocol) peern.getProtocol(typePid);
            //if ((getEpoch() % ntype.getFrequency(node.getID())) == 0 ){
            if (!peern.isUp()) {
                return;
            }
            Vector<EventUID> queueToSend = queue.get(peern.getID());
            //System.out.println("Sending queue "+queueToSend+" to node "+peern.getID()+" from node "+node.getID());
            if (queueToSend != null) {
                sendMessage(node, peern, new QueueMessage((Vector<EventUID>) queueToSend.clone(), ntype.getType(), getEpoch(), node.getID()), pid);
                this.cleanQueue(peern.getID());
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

            //TODO Epa isto atm é necessario porque os brokers tambem podem ser visto como
            // destino de migrações. Este if resolve o problema
            if (((TypeProtocol) node.getProtocol(typePid)).getType() == Type.BROKER) {
                continue;
            }

            if (isInterested(node.getID(), event.getOperation().getKey()) && node.isUp()) {
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
            //System.out.println("DATA MESSAGE");
            DataMessage msg = (DataMessage) event;
            this.addData(msg.event, msg.data);
        } else if (event instanceof MetadataMessage) {
            //System.out.println("METADATA");
            MetadataMessage msg = (MetadataMessage) event;
            //System.out.println("Node "+node.getID()+"in epoch "+getEpoch()+" has received a metadata "+msg.event.toString()+" from Node "+msg.senderId+" in epoch "+msg.epoch);
            if (msg.epoch == this.getEpoch()) {
                //System.out.println("About to insert to the queue");
                this.addToQueue(msg.event, msg.senderId);
            } else {
                this.addToPendingQueue(msg.event, msg.epoch, msg.senderId);
            }

            /* ************** BROKERS ****************** */
        } else if (event instanceof MigrationMessage) {
            MigrationMessage msg = (MigrationMessage) event;
            //    System.out.println("Received migration for " + msg.destinationId + ", I am node " + node.getID());
            Vector<EventUID> vec = new Vector<>();
            vec.add(msg.event);
            if (msg.epoch == this.getEpoch()) {
                this.addQueueToQueue(vec, msg.senderId);
            } else {
                this.addQueueToPendingQueue(vec, msg.epoch, msg.senderId);
            }
        } else if (event instanceof QueueMessage) {
            QueueMessage msg = (QueueMessage) event;
            //    System.out.println("Node "+node.getID()+" has received a queue "+msg.queue.toString()+" from Node "+msg.senderId);
            if (msg.type == Type.DATACENTER) {
                this.processQueue(msg.queue, node.getID());
                //System.out.println("queue node "+node.getID()+": "+metadataQueue.toString());
                //System.out.println("data vv "+node.getID()+": "+data.getVector().toString());
            } else {
                if (msg.epoch == this.getEpoch()) {
                    this.addQueueToQueue(msg.queue, msg.senderId);
                } else {
                    this.addQueueToPendingQueue(msg.queue, msg.epoch, msg.senderId);
                }
            }
        }
    }

//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

    class DataMessage {

        final EventUID event;
        final Object data;
        final int epoch;

        public DataMessage(EventUID event, Object data, int epoch) {
            this.event = event;
            this.data = data;
            this.epoch = epoch;
        }
    }

    class MetadataMessage {

        final EventUID event;
        final int epoch;
        final long senderId;

        public MetadataMessage(EventUID event, int epoch, long senderId) {
            this.event = event;
            this.epoch = epoch;
            this.senderId = senderId;
        }

    }

    class MigrationMessage {

        final EventUID event;
        final long senderId;
        final int epoch;

        public MigrationMessage(EventUID event, long senderId, int epoch) {
            this.event = event;
            this.senderId = senderId;
            this.epoch = epoch;
        }
    }


    class QueueMessage {

        final Vector<EventUID> queue;
        final Type type;
        final int epoch;

        final long senderId;

        public QueueMessage(Vector<EventUID> queue, Type type, int epoch, long senderId) {
            this.queue = queue;
            this.type = type;
            this.epoch = epoch;
            this.senderId = senderId;
        }
    }

    class OutwardsPartitionObject {

        final Node src;
        final Node dest;
        final Object msg;
        final int pid;

        OutwardsPartitionObject(Node src, Node dest, Object msg, int pid) {
            this.src = src;
            this.dest = dest;
            this.msg = msg;
            this.pid = pid;
        }
    }

}

