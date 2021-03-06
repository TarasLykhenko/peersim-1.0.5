package example.occult.temporal_compression;

import example.common.MigrationMessage;
import example.common.PointToPointTransport;
import example.common.datatypes.Operation;
import example.occult.GroupsManager;
import example.occult.OccultClientInterface;
import example.occult.StateTreeProtocol;
import example.occult.datatypes.OccultMasterWrite;
import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.ReadOperation;
import example.occult.datatypes.UpdateOperation;
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


public class OccultIdenticalTreeProtocol extends StateTreeProtocolInstance
        implements CDProtocol, EDProtocol {

    private static final String PAR_TREE_PROT = "tree_protocol";
    private final int tree;
    private final String prefix;


//--------------------------------------------------------------------------
// Initialization
//--------------------------------------------------------------------------

    public OccultIdenticalTreeProtocol(String prefix) {
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
        StateTreeProtocolInstance datacenter = (StateTreeProtocolInstance) node.getProtocol(tree);
        // System.out.println("Checking node " + node.getID() + " clients");
        for (OccultClientInterface client : clients) {
         //   System.out.println("Checking client " + client.getId());
            Operation operation = client.nextOperation();
            // Client is waiting for result;
            if (operation == null) {
                //System.out.println("Client " + client.getId() + " is waiting!");
                continue;
            }

            // DC doesn't have key, migrate the client
            if (!datacenter.isInterested(operation.getKey())) {
                // System.out.println("DC not interested, migrating client " + client.getId());
                MigrationMessage msg = new MigrationMessage(datacenter.getNodeId(), client.getId());
                Node migrationDatacenter = getMigrationDatacenter(operation.getKey(), datacenter);
                client.migrationStart();
                sendMessage(node, migrationDatacenter, msg, pid);
                sentMigrations++;
                continue;
            }


            // If is Local Read
            if (eventIsRead(operation)) {
                ReadOperation readOperation = (ReadOperation) operation;
                if (readOperation.migrateToMaster()) {
                //    System.out.println("Client (" + client.getId() + " wants to migrate. ");
                    migrateToMaster(node, pid, client, operation.getKey());
                } else {
                    ReadMessage readMessage = new ReadMessage(this.getNodeId(), client.getId(), operation.getKey());
                    sendMessage(node, node, readMessage, pid);
                }
                continue;
            }

            // If is local update
            if (eventIsUpdate(operation)) {
                // Check if local DC is the shardmaster, otherwise migrate the client to it.
                int shardId = GroupsManager.getInstance().getShardId(operation.getKey());
                StateTreeProtocol masterServer = GroupsManager.getInstance().getMasterServer(shardId);
                if (this.getNodeId() != masterServer.getNodeId()) {
                 //   System.out.println("Am not master, migrating");
                    migrateToMaster(node, pid, client, operation.getKey());
                    continue;
                }

                UpdateOperation updateOperation = (UpdateOperation) operation;

                LocalUpdate localUpdate = new LocalUpdate(client.getId(),
                        datacenter.getNodeId(),
                        updateOperation.getKey(),
                        updateOperation.getDeps(),
                        updateOperation.getCatchAll());

                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }
    }

    private Node getMigrationDatacenter(int key, StateTreeProtocol originalDC) {

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

    private void migrateToMaster(Node node, int pid,
                                 OccultClientInterface client, int key) {
        int shardId = GroupsManager.getInstance().getShardId(key);
        StateTreeProtocol master = GroupsManager.getInstance().getMasterServer(shardId);
        // System.out.println("Client " + client.getId() + " is master Migrating from " + node.getID() + " to " + master.getNodeId());

        Node targetDC = Network.get(Math.toIntExact(master.getNodeId()));
        MigrationMessage msg = new MigrationMessage(node.getID(), client.getId());
        client.migrationStart();
        if (targetDC.getID() == node.getID()) {
            throw new RuntimeException("Migrating from node A to node A?");
        }
        sendMessage(node, targetDC, msg, pid);
    //    System.out.println("Migrating " + client.getId() + " from " + datacenter.getNodeId() + " to " + master.getNodeId());
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
            OccultClientInterface client = idToClient.get(localUpdate.clientId);

            // This must be the master
            OccultMasterWrite occultMasterWrite =
                    this.occultWriteMaster(localUpdate.key, localUpdate.deps, localUpdate.catchAll);

            int shardId = GroupsManager.getInstance().getShardId(localUpdate.key);
            Set<StateTreeProtocol> slaves = GroupsManager.getInstance().getShardSlaves(shardId);

            // Update slaves
            for (StateTreeProtocol slave : slaves) {
                RemoteUpdateMessage remoteUpdate = new RemoteUpdateMessage(
                        this.getNodeId(),
                        localUpdate.key,
                        occultMasterWrite.getDeps(),
                        occultMasterWrite.getCatchAll(),
                        occultMasterWrite.getShardStamp()
                );

                Node targetNode = Network.get(Math.toIntExact(slave.getNodeId()));
                sendMessage(node, targetNode, remoteUpdate, pid);
            }

            client.occultReceiveUpdateResult(shardId, occultMasterWrite.getShardStamp());
        }

        if (event instanceof RemoteUpdateMessage) {
            RemoteUpdateMessage msg = (RemoteUpdateMessage) event;

            if (msg.senderDC == nodeId) {
                throw new RuntimeException("Remote LOCAL update?");
            }

            this.occultWriteSlave(msg.key, msg.deps, msg.catchAll, msg.shardStamp);
        }

        if (event instanceof MigrationMessage) {
            MigrationMessage msg = (MigrationMessage) event;
            StateTreeProtocolInstance originalDC = (StateTreeProtocolInstance)
                    Network.get(Math.toIntExact(msg.senderDC)).getProtocol(tree);

            OccultClientInterface client = originalDC.idToClient.get(msg.clientId);
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
            this.idToClient.get(msg.clientId).occultReceiveReadResult(this.nodeId, read);
        }
    }

    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------
    //--------------------------------------------------------------------------


    public Object clone() {
        return new OccultIdenticalTreeProtocol(prefix);
    }

    private boolean eventIsRead(Operation operation) {
        return operation.getType() == Operation.Type.READ;
    }

    private boolean eventIsUpdate(Operation operation) {
        return operation.getType() == Operation.Type.UPDATE;
    }

    /*
    private void debugCheckIfNodeIsPartitioned(Node bestNode) {
        Map<Long, Integer> longIntegerMap = PointToPointTransport.partitionDCTable.get(bestNode.getID());
        for (Long dstNode : longIntegerMap.keySet()) {
            if (longIntegerMap.get(dstNode) > CommonState.getTime()) {
                System.out.println("MIGRATING TO PARTITIONED NODE!" + longIntegerMap.get(dstNode) + " | " + CommonState.getTime());
            }
        }
    }
    */

    @Override
    public int getQueuedClients() {
        return 0;
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
        final long datacenterId; // for debug
        final int key;
        final Map<Integer, Integer> deps;
        final int catchAll;
        final long timestamp;

        LocalUpdate(int clientId, long datacenterId, int key, Map<Integer, Integer> deps, int catchAll) {
            this.clientId = clientId;
            this.datacenterId = datacenterId;
            this.key = key;
            this.deps = deps;
            this.catchAll = catchAll;
            timestamp = CommonState.getTime();
        }

    }

    class RemoteUpdateMessage {

        final long senderDC;
        final int key;
        // final EventUID event;
        final Map<Integer, Integer> deps;
        final int catchAll;
        final int shardStamp;
        final long timestamp;

        RemoteUpdateMessage(long sender,
                            int key,
                            // EventUID event,
                            Map<Integer, Integer> deps,
                            int catchAll,
                            int shardStamp) {
            this.senderDC = sender;
            this.key = key;
            //this.event = event;
            this.deps = deps;
            this.catchAll = catchAll;
            this.shardStamp = shardStamp;
            timestamp = CommonState.getTime();
        }
    }
}


