package example.occult;

import example.occult.datatypes.EventUID;
import example.occult.datatypes.OccultMasterWrite;
import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.Operation;
import example.occult.datatypes.ReadOperation;
import example.occult.datatypes.UpdateOperation;
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
         //   System.out.println("Checking client " + client.getId());
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                //System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }
            EventUID event = new EventUID(operation, CommonState.getTime(), datacenter.getNodeId(), 0);

            // DC doesn't have key, migrate the client
            if (!datacenter.isInterested(operation.getKey())) {
                System.out.println("DC not interested, migrating");
                startMigration(node, pid, datacenter, client, event);
                continue;
            }


            // If is Local Read
            if (eventIsRead(event)) {
                ReadOperation readOperation = (ReadOperation) operation;
                if (readOperation.migrateToMaster()) {
                    System.out.println("Client (" + client.getId() + " wants to migrate. ");
                    startMigration(node, pid, datacenter, client, event);
                } else {
                    ReadMessage readMessage = new ReadMessage(this.getNodeId(), client.getId(), operation.getKey());
                    sendMessage(node, node, readMessage, pid);
                }
                continue;
            }

            // If is local update
            if (eventIsUpdate(event)) {
                // Check if local DC is the shardmaster, otherwise migrate the client to it.
                int shardId = GroupsManager.getInstance().getShardId(event.getOperation().getKey());
                StateTreeProtocol masterServer = GroupsManager.getInstance().getMasterServer(shardId);
                if (this.getNodeId() != masterServer.getNodeId()) {
                    System.out.println("Am not master, migrating");
                    startMigration(node, pid, datacenter, client, event);
                    continue;
                }

                UpdateOperation updateOperation = (UpdateOperation) event.getOperation();

                LocalUpdate localUpdate = new LocalUpdate(client.getId(),
                        datacenter.getNodeId(),
                        updateOperation.getKey(),
                        updateOperation.getUpdateContext());

                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }
    }

    /**
     * In occult, clients only want to migrate to the masters.
     * @param node
     * @param pid
     * @param datacenter
     * @param client
     * @param event
     */
    private void startMigration(Node node, int pid,
                                StateTreeProtocolInstance datacenter,
                                Client client, EventUID event) {
        int key = event.getOperation().getKey();
        int shardId = GroupsManager.getInstance().getShardId(key);
        StateTreeProtocol master = GroupsManager.getInstance().getMasterServer(shardId);

        Node targetDC = Network.get(Math.toIntExact(master.getNodeId()));
        MigrationMessage msg = new MigrationMessage(datacenter.getNodeId(), client.getId());
        client.startMigration();
        sendMessage(node, targetDC, msg, pid);
        System.out.println("Migrating " + client.getId() + " from " + datacenter.getNodeId() + " to " + master.getNodeId());
        sentMigrations++;
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
            Client client = idToClient.get(localUpdate.clientId);

            // This must be the master
            OccultMasterWrite occultMasterWrite =
                    this.occultWriteMaster(localUpdate.key, localUpdate.deps);

            int shardId = GroupsManager.getInstance().getShardId(localUpdate.key);
            Set<StateTreeProtocol> slaves = GroupsManager.getInstance().getShardSlaves(shardId);

            // Update slaves
            for (StateTreeProtocol slave : slaves) {
                RemoteUpdateMessage remoteUpdate = new RemoteUpdateMessage(
                        this.getNodeId(),
                        localUpdate.key,
                        occultMasterWrite.getDeps(),
                        occultMasterWrite.getShardStamp()
                );

                Node targetNode = Network.get(Math.toIntExact(slave.getNodeId()));
                sendMessage(node, targetNode, remoteUpdate, pid);
            }

            client.receiveUpdateResult(shardId, occultMasterWrite.getShardStamp());
        }

        if (event instanceof RemoteUpdateMessage) {
            RemoteUpdateMessage msg = (RemoteUpdateMessage) event;

            if (msg.senderDC == nodeId) {
                throw new RuntimeException("Remote LOCAL update?");
            }

            this.occultWriteSlave(msg.key, msg.deps, msg.shardStamp);
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
            //System.out.println("Node " + node.getID() + " received client " + client.getId());
            this.migrateClientQueue(client);
        }

        if (event instanceof ReadMessage) {
            ReadMessage msg = (ReadMessage) event;
            if (msg.senderDC != nodeId) {
                throw new RuntimeException("Reads must ALWAYS be local.");
            }
            OccultReadResult read = occultRead(msg.key);
            this.idToClient.get(msg.clientId).receiveReadResult(this.nodeId, read);
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
        final long datacenterId; // for debug
        final int key;
        final Map<Integer, Integer> deps;
        final long timestamp;

        LocalUpdate(int clientId, long datacenterId, int key, Map<Integer, Integer> deps) {
            this.clientId = clientId;
            this.datacenterId = datacenterId;
            this.key = key;
            this.deps = deps;
            timestamp = CommonState.getTime();
        }

    }

    class RemoteUpdateMessage {

        final long senderDC;
        final int key;
        // final EventUID event;
        final Map<Integer, Integer> deps;
        final int shardStamp;
        final long timestamp;

        RemoteUpdateMessage(long sender,
                            int key,
                            // EventUID event,
                            Map<Integer, Integer> deps,
                            int shardStamp) {
            this.senderDC = sender;
            this.key = key;
            //this.event = event;
            this.deps = deps;
            this.shardStamp = shardStamp;
            timestamp = CommonState.getTime();
        }
    }
}


