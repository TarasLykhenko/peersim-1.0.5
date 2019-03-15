package example.capstonematrix;

import example.capstonematrix.datatypes.HRC;
import example.capstonematrix.datatypes.ReadResult;
import example.capstonematrix.datatypes.UpdateMessage;
import example.capstonematrix.datatypes.UpdateResult;
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

import java.util.HashSet;
import java.util.Set;

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
        datacenter = Configuration.getPid("tree");
    }

//--------------------------------------------------------------------------
// methods
//--------------------------------------------------------------------------

    /**
     * Every client attempts to do something.
     */
    @Override
    public void nextCycle(Node node, int pid) {
        heartbeat(node, pid);

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
                        operation.getKey());
                sendMessage(node, node, localUpdate, pid);
            } else {
                System.out.println("Unknown scenario!");
            }
        }
    }

    private long timeSinceLastHeartBeat = 0;
    private long heartbeatPeriod = 150; //TODO TIRAR ESTE HARDCODED
    private void heartbeat(Node node, int pid) {
        long currentTime = CommonState.getTime();
        if (currentTime - timeSinceLastHeartBeat > heartbeatPeriod) {
            sendHeartbeat(node, pid);
            timeSinceLastHeartBeat = currentTime;
        }
    }

    private void sendHeartbeat(Node node, int pid) {
        Heartbeat heartbeat = new Heartbeat(nodeId, cloudletLC);

        for (int i = 0; i < Network.size(); i++) {
            if (i == nodeId) {
                continue;
            }
            Node targetNode = Network.get(i);
            sendMessage(node, targetNode, heartbeat, pid);
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
                new MigrationMessage(originalDC.getID(), client.getId(), client.getClientHRC());

        client.migrationStart();
        sendMessage(originalDC, migrationTarget, migrationMessage, pid);
        sentMigrations++;
    }

    private Set<Node> getInterestedDatacenters(int key) {
        Set<Node> interestedNodes = new HashSet<>();
        // First get which datacenters replicate the data
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);

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

        if (event instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) event;
            updateLastReceived(heartbeat.originDC, heartbeat.cloudletLC);
            checkRemoteUpdatesTable();
            checkMigrationTable(heartbeat.originDC);
        }

        // Local read
        if (event instanceof ReadMessage) {
            ReadMessage msg = (ReadMessage) event;
            if (msg.senderDC != nodeId) {
                throw new RuntimeException("Reads must ALWAYS be local.");
            }

            ReadResult readResult = this.capstoneRead(msg.key);

            this.idToClient.get(msg.clientId).receiveReadResult(msg.key, readResult);
        }

        // Local update
        // HRCc : HRC of client | HRCv = HRC of value

        // 1 - Client reads current value HRCc
        // 2 - Client merges HRCv with its own HRCc (producing HRC')
        // 3 - Client increments CloudletLC and sets updateLC = CloudletLC
        // 4 - An update Message is generated with:
        //      - Source cloudlet ID
        //      - Data object key
        //      - updateLC
        //      - HRC'
        // 5 - Client generates a new HRC'', that INCORPORATES updateLC
        // 6 - The update is stored with HRC'' and the client's HRC becomes HRC''
        // 7 - The update is spread
        if (event instanceof LocalUpdate) {
            LocalUpdate msg = (LocalUpdate) event;

            // Step 1 and 2
            ReadResult readResult = this.capstoneRead(msg.key);
            Client client = this.idToClient.get(msg.clientId);
            client.handleReadResult(msg.key, readResult);

            // Step 3
            cloudletLC++;
            int updateLC = cloudletLC;

            // Step 5
            HRC incorporatedHRC = client.getClientHRC().incorporate(msg.key, nodeId, updateLC);

            // Step 6
            this.capstoneWrite(msg.key, incorporatedHRC);
            updateLastReceived(nodeId, updateLC);

            // Step 7
            Set<Node> interestedDatacenters = getInterestedDatacenters(msg.key);
            // Remove self from remote update list
            interestedDatacenters.remove(Network.get(Math.toIntExact(this.getNodeId())));
            for (Node interestedNode : interestedDatacenters) {
                UpdateMessage update = new UpdateMessage(
                        nodeId, msg.key, updateLC, client.getClientHRC());
                //System.out.println("Sending remote past:");
                //update.getPast().print();
                sendMessage(node, interestedNode, update, pid);
            }

            UpdateResult result = new UpdateResult(nodeId, incorporatedHRC);
            client.receiveUpdateResult(msg.key, result);
        }

        // Remote update
        if (event instanceof UpdateMessage) {
            UpdateMessage updateMessage = (UpdateMessage) event;

            if (!isInterested(updateMessage.getKey())) {
                throw new RuntimeException("wow");
            }

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

            migrateClient(msg.originDC, client, msg.clientHRC);
        }
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
        final long timestamp;

        LocalUpdate(int clientId, int key) {
            this.clientId = clientId;
            this.key = key;
            timestamp = CommonState.getTime();
        }
    }

    class MigrationMessage {

        final long originDC;
        final int clientId;
        final HRC clientHRC;
        final long timestamp;

        MigrationMessage(long originDC, int clientId, HRC clientHRC) {
            this.originDC = originDC;
            this.clientId = clientId;
            this.clientHRC = new HRC(clientHRC);
            this.timestamp = CommonState.getTime();
        }
    }

    class Heartbeat {

        final long originDC;
        final int cloudletLC;

        Heartbeat(long originDC, int cloudletLC) {
            this.originDC = originDC;
            this.cloudletLC = cloudletLC;
        }
    }

}

