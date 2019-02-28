package example.capstone;

import example.capstone.datatypes.ReadResult;
import example.capstone.datatypes.UpdateMessage;
import example.capstone.datatypes.UpdateResult;
import example.common.PointToPointTransport;
import example.common.datatypes.Operation;
import peersim.cdsim.CDProtocol;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static example.capstone.ProtocolMapperInit.Type.BROKER;

public class DatacenterProtocol extends DatacenterProtocolInstance
        implements CDProtocol, EDProtocol {

    private final int datacenter;
    private final String prefix;


//--------------------------------------------------------------------------
// Initialization
//--------------------------------------------------------------------------

    public DatacenterProtocol(String prefix) {
        super(prefix);
        this.prefix = prefix;
        datacenter = Configuration.getPid("datacenter");
    }

//--------------------------------------------------------------------------
// methods
//--------------------------------------------------------------------------

    /**
     * Every client attempts to do something.
     */
    @Override
    public void nextCycle(Node node, int pid) {
        for (Client client : clients) {
            Operation operation = client.nextOperation();
            if (operation == null) {
                continue;
            }

            // If the datacenter does not have the object, migrate it
            if (!this.isInterested(operation.getKey())) {
                migrateClientStart(client, operation.getKey(), node, pid);
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
                LocalUpdate localUpdate = new LocalUpdate(client.getId(),
                        operation.getKey(), client.getClientClock());
                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }
    }

    private void migrateClientStart(Client client,
                                    int key,
                                    Node originalDC,
                                    int pid) {

        // Get datacenters that have the key
        Set<Node> interestedNodes = getInterestedDatacenters(key);

        // Then select the datacenter that has the lowest latency to the client
        Node migrationTarget = getLowestLatencyDatacenter(interestedNodes);

        MigrationMessage migrationMessage =
                new MigrationMessage(originalDC.getID(), client.getId(), client.getClientClock());

        client.migrationStart();
        sendMessage(originalDC, migrationTarget, migrationMessage, pid);
        sentMigrations++;
    }

    private Set<Node> getInterestedDatacenters(int key) {
        Set<Node> interestedNodes = new HashSet<>();
        // First get which datacenters replicate the data
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);

            // Can't migrate to a broker
            if (ProtocolMapperInit.nodeType.get(node.getID()) == BROKER) {
                continue;

            }

            StateTreeProtocol dc = (StateTreeProtocol) node.getProtocol(this.datacenter);

            if (dc.isInterested(key)) {
                interestedNodes.add(node);
            }
        }
        return interestedNodes;
    }

    private Node getLowestLatencyDatacenter(Set<Node> interestedNodes) {
        int lowestLatency = Integer.MAX_VALUE;
        Node bestNode = null;
        for (Node interestedNode : interestedNodes) {
            int nodeLatency = PointToPointTransport.staticGetLatency(this.nodeId, interestedNode.getID());
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

    private boolean eventIsRead(Operation operation) {
        return operation.getType() == Operation.Type.READ;
    }

    private boolean eventIsUpdate(Operation operation) {
        return operation.getType() == Operation.Type.UPDATE;
    }


//--------------------------------------------------------------------------

    /**
     * This is the standard method to define to process incoming messages.
     */
    public void processEvent(Node node, int pid, Object event) {

        /* ************** DATACENTERS ****************** */

        // Local read
        if (event instanceof ReadMessage) {
            ReadMessage msg = (ReadMessage) event;
            if (msg.senderDC != nodeId) {
                throw new RuntimeException("Reads must ALWAYS be local.");
            }
            ReadResult readResult = this.capstoneRead(msg.key);

            this.idToClient.get(msg.clientId).receiveReadResult(msg.key, readResult);
        }

        // Local update - Must generate remote update
        if (event instanceof LocalUpdate) {
            LocalUpdate localUpdate = (LocalUpdate) event;

            UpdateResult result = this.capstoneWrite(localUpdate.key, localUpdate.clientClock);
            Client client = idToClient.get(localUpdate.clientId);
            client.receiveUpdateResult(localUpdate.key, result);

            ReadResult readResult = this.capstoneRead(localUpdate.key);
            UpdateMessage updateMessage =
                    new UpdateMessage(this.nodeId, localUpdate.key,
                            readResult.getLocalDataClock(), this.nodeId);

            sendUpdateMessageToBroker(node, updateMessage, pid);
        }

        // Remote update
        if (event instanceof UpdateMessage) {
            UpdateMessage updateMessage = (UpdateMessage) event;
            // if (!isInterested(updateMessage.getKey())) throw new RuntimeException("wow");
            this.processRemoteUpdate(updateMessage);
        }

        if (event instanceof MigrationMessage) {
            MigrationMessage msg = (MigrationMessage) event;
            DatacenterProtocol originalDC = (DatacenterProtocol)
                    Network.get(Math.toIntExact(msg.originDC)).getProtocol(datacenter);

            Client client = originalDC.idToClient.get(msg.clientId);
            // Remove client from original DC
            originalDC.clients.remove(client);
            originalDC.idToClient.remove(msg.clientId);

            migrateClient(msg.originDC, client, msg.clientClock);
        }
    }

    private void sendUpdateMessageToBroker(Node node, UpdateMessage updateMessage, int pid) {
        long parentId = GroupsManager.getInstance().getTreeOverlay().getParent(this.nodeId);
        sendMessage(node, Network.get(Math.toIntExact(parentId)), updateMessage, pid);
    }

    @Override
    public Object clone() {
        return new DatacenterProtocol(prefix);
    }

//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

    class ReadMessage {

        final long senderDC;
        final int clientId;
        final int key;
        final long timestamp;

        ReadMessage(long senderDC, int clientId, int key) {
            this.senderDC = senderDC;
            this.clientId = clientId;
            this.key = key;
            timestamp = CommonState.getTime();
        }
    }

    class LocalUpdate {

        final int clientId;
        final int key;
        final Map<Long, Integer> clientClock;
        final long timestamp;

        LocalUpdate(int clientId, int key, Map<Long, Integer> clientClock) {
            this.clientId = clientId;
            this.key = key;
            this.clientClock = clientClock;
            timestamp = CommonState.getTime();
        }
    }

    class MigrationMessage {

        final long originDC;
        final int clientId;
        final Map<Long, Integer> clientClock;
        final long timestamp;

        MigrationMessage(long originDC, int clientId, Map<Long, Integer> clientClock) {
            this.originDC = originDC;
            this.clientId = clientId;
            this.clientClock = new HashMap<>(clientClock);
            this.timestamp = CommonState.getTime();
        }
    }

}

