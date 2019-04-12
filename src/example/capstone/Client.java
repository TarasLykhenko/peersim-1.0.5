package example.capstone;

import example.capstone.datatypes.ReadOperation;
import example.capstone.datatypes.ReadResult;
import example.capstone.datatypes.UpdateOperation;
import example.capstone.datatypes.UpdateResult;
import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static example.capstone.DatacenterProtocolInstance.*;

public class Client extends AbstractBaseClient {

    private Map<Long, Integer> clientClock = new HashMap<>();

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality, GroupsManager groupsManager) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality, groupsManager);
        List<Long> nodesToRoot = GroupsManager.getInstance()
                .getTreeOverlay()
                .getNodesOnPathToRoot(Math.toIntExact(datacenter.getNodeId()));
        for (Long entry : nodesToRoot) {
            clientClock.put(entry, 0);
        }
    }

    Map<Long, Integer> getClientClock() {
        return new HashMap<>(clientClock);
    }

    @Override
    public boolean doExtraBehaviour() {
        return false;
    }

    @Override
    public Operation specificDoRead(DataObject dataObject) {
        return new ReadOperation(dataObject.getKey());
    }

    @Override
    public Operation specificDoUpdate(DataObject dataObject) {
        return new UpdateOperation(dataObject.getKey(), clientClock);
    }

    @Override
    public void handleReadResult(int key, Object readResult) {
        ReadResult read = (ReadResult) readResult;
        this.clientClock = getEntryWiseMaxClock(clientClock, read.getLocalDataClock());
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        UpdateResult result = (UpdateResult) updateResult;

        if (clientClock.get(result.getCloudletId()) > result.getCloudletCounter()) {
            throw new RuntimeException("Impossible scenario");
        }

        clientClock.put(result.getCloudletId(), result.getCloudletCounter());
    }

    public void migrationOver(long dcId, Map<Long, Integer> cloudletClock) {
        migrationOver(dcId);
        this.clientClock = new HashMap<>(cloudletClock);
    }

}
