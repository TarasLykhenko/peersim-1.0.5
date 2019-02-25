package example.occult;

import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;
import example.occult.datatypes.OccultReadResult;
import example.occult.datatypes.ReadOperation;
import example.occult.datatypes.UpdateOperation;
import peersim.config.Configuration;
import peersim.core.CommonState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class OccultClient extends AbstractBaseClient
        implements OccultClientInterface {

    private static final String PAR_CLIENT_NUMBER_RETRIES = "client_read_retries";
    private static final String PAR_CLIENT_RETRY_INTERVAL = "client_read_retry_interval";

    private static final int NUMBER_RETRIES;
    private static final int RETRY_INTERVAL;

    static {
        NUMBER_RETRIES = Configuration.getInt(PAR_CLIENT_NUMBER_RETRIES);
        RETRY_INTERVAL = Configuration.getInt(PAR_CLIENT_RETRY_INTERVAL);
    }


    private boolean hadStaleRead = false;
    private int totalStaleReads = 0;
    private int totalNumberMasterMigration = 0;
    protected int totalCatchAllReads = 0;
    private int numberStaleReads = 0;

    /**
     * Maps each shardId to shardstamp
     */
    protected Map<Integer, Integer> clientTimestamp = new HashMap<>();
    protected int catchAllShardStamp = 0;

    public OccultClient(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);
    }

    @Override
    public boolean doExtraBehaviour() {
        // System.out.println("Client " + getId() + " action");
        if (hadStaleRead) {
            int lastOperationKey = lastOperation.getKey();
            // System.out.println("Client + " + getId() + " had stale read with " + numberStaleReads);
            if (numberStaleReads == NUMBER_RETRIES) {
                int shardId = GroupsManager.getInstance().getShardId(lastOperationKey);
                long master = GroupsManager.getInstance().getMasterServer(shardId).getNodeId();
                // System.out.println("I am client " + getId() + ", want to migrate to master " + master);
                totalNumberMasterMigration++;
                // System.out.println("Client migrating to master (" + master + ") for read.");
                lastOperation = new ReadOperation(lastOperationKey, true);
            } else {
                lastOperation = new ReadOperation(lastOperationKey, false);
            }
            isWaitingForResult = true;
            waitingSince = CommonState.getTime();

            return true;
        } else {
            return false;
        }
    }

    @Override
    public Operation specificDoRead(int readLevel) {
        DataObject randomDataObject = chooseRandomDataObject(readLevel);
        return new ReadOperation(randomDataObject.getKey(), false);
    }

    @Override
    public Operation specificDoUpdate(int updateLevel) {
        DataObject randomDataObject = chooseRandomDataObject(updateLevel);
        return new UpdateOperation(randomDataObject.getKey(), clientTSCopy(), getNumberCatchAll());
    }


    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    @Override
    public void occultReceiveReadResult(long server, OccultReadResult readResult) {
        boolean readFromSlave = !readResult.isMaster();
        int shardId = readResult.getShardId();
        int clientShardStamp = getShardStampFromCS(shardId);
        int shardStamp = readResult.getShardStamp();

        if (readFromSlave && shardStamp < clientShardStamp) {
            lastResultReceivedTimestamp = CommonState.getTime();
            nextRestTime = RETRY_INTERVAL;
            numberStaleReads++;
            totalStaleReads++;
            hadStaleRead = true;
        } else {
            numberStaleReads = 0;
            hadStaleRead = false;
            lastResultReceivedTimestamp = CommonState.getTime();
            readsTotalLatency += (lastResultReceivedTimestamp - lastOperationTimestamp);

            updateClientTimestampFromRead(clientTimestamp, catchAllShardStamp,
                    readResult.getDeps(), readResult.getCatchAll());
        }
        isWaitingForResult = false;
    }

    public abstract int getShardStampFromCS(int shardId);

    public abstract void updateClientTimestampFromRead(
            Map<Integer, Integer> clientTimestamp,
            int clientCatchAll,
            Map<Integer, Integer> deps,
            int depsCatchAll);


    private Map<Integer, Integer> clientTSCopy() {
        return new HashMap<>(clientTimestamp);
    }

    @Override
    public void handleReadResult(int key, Object readResult) {
        // Don't use this
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        // Don't use this
    }


    @Override
    public int getNumberMasterMigrations() {
        return totalNumberMasterMigration;
    }

    @Override
    public int getNumberCatchAll() {
        return totalCatchAllReads;
    }

    @Override
    public int getNumberStaleReads() {
        return totalStaleReads;
    }

    @Override
    public void migrationOver() {
        super.migrationOver();
        Operation op = lastOperation;
        if (op instanceof ReadOperation) {
            ReadOperation ro = (ReadOperation) op;
            if (ro.migrateToMaster()) {
                ro.setMigration(false);
            }
        }
    }
}
