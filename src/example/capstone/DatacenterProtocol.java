package example.capstone;

import example.capstone.datatypes.ReadResult;
import example.capstone.datatypes.UpdateMessage;
import example.capstone.datatypes.UpdateResult;
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
import java.util.Map;

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
        heartbeat(node, pid);

        for (Client client : clients) {
            Operation operation = client.nextOperation();
            if (operation == null) {
                continue;
            }

            // If is remote operation, migrate
            if (operation.getDatacenter() != nodeId) {
                migrateClientStart(client, node, operation.getDatacenter(), pid);
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

    private long timeSinceLastHeartBeat = 0;
    private long heartbeatPeriod = 50; //TODO TIRAR ESTE HARDCODED
    private void heartbeat(Node node, int pid) {
        long currentTime = CommonState.getTime();
        if (currentTime - timeSinceLastHeartBeat > heartbeatPeriod) {
            sendHeartbeat(node, pid);
            timeSinceLastHeartBeat = currentTime;
        }
    }

    private void sendHeartbeat(Node node, int pid) {

        //TODO NOTA! Isto aqui tem tamanho fixo 8 porque os primeiros 8 nos
        // sao servidores, enquanto que os outros sao brokers.
        for (int i = 0; i < 8; i++) {
            if (i == nodeId) {
                continue;
            }
            Heartbeat heartbeat = new Heartbeat(nodeId, cloudletClock);
            Node targetNode = Network.get(i);
            sendMessage(node, targetNode, heartbeat, pid);
        }
    }

    private void migrateClientStart(Client client,
                                    Node originalDC,
                                    long targetDC,
                                    int pid) {

        Node migrationTarget = Network.get((int) targetDC);

        MigrationMessage migrationMessage =
                new MigrationMessage(originalDC.getID(), client.getId(), client.getClientClock());

        client.migrationStart();
        sendMessage(originalDC, migrationTarget, migrationMessage, pid);
        sentMigrations++;
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

        if (event instanceof Heartbeat) {
            Heartbeat heartbeat = (Heartbeat) event;
            processHeartbeat(heartbeat.originDC, heartbeat.cloudletLC);
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

            this.processRemoteUpdate(updateMessage);
            sendMessageToBroker(node, updateMessage, pid);
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

    private void sendMessageToBroker(Node node, Object message, int pid) {
        long parentId = GroupsManager.getInstance().getTreeOverlay().getParent(this.nodeId);
        sendMessage(node, Network.get(Math.toIntExact(parentId)), message, pid);
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

    class Heartbeat {

        final long originDC;
        final Map<Long, Integer> cloudletLC;
        final long timestamp;

        Heartbeat(long originDC, Map<Long, Integer> cloudletLC) {
            this.originDC = originDC;
            this.cloudletLC = new HashMap<>(cloudletLC);
            this.timestamp = CommonState.getTime();
        }
    }

}

