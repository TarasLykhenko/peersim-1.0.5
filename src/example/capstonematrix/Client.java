package example.capstonematrix;

import example.capstonematrix.datatypes.ReadOperation;
import example.capstonematrix.datatypes.ReadResult;
import example.capstonematrix.datatypes.UpdateOperation;
import example.capstonematrix.datatypes.UpdateResult;
import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static example.capstone.DatacenterProtocolInstance.getEntryWiseMaxClock;

public class Client extends AbstractBaseClient {

    private List<Map<Long, Integer>> clientPast = new ArrayList<>();

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);
        List<Long> nodesToRoot = GroupsManager.getInstance()
                .getTreeOverlay()
                .getNodesOnPathToRoot(Math.toIntExact(datacenter.getNodeId()));

        // TODO
        /*
        for (Long entry : nodesToRoot) {
            clientHRC.put(entry, 0);
        }
        */
    }

    List<Map<Long, Integer>> getClientClock() {
        return new ArrayList<>(clientPast);
    }

    @Override
    public boolean doExtraBehaviour() {
        return false;
    }

    @Override
    public Operation specificDoRead(int readLevel) {
        DataObject randomDataObject = chooseRandomDataObject(readLevel);
        return new ReadOperation(randomDataObject.getKey());
    }

    @Override
    public Operation specificDoUpdate(int updateLevel) {
        DataObject randomDataObject = chooseRandomDataObject(updateLevel);
        return new UpdateOperation(randomDataObject.getKey(), clientPast);
    }

    @Override
    public void handleReadResult(int key, Object readResult) {
        ReadResult read = (ReadResult) readResult;
        this.clientClock = getEntryWiseMaxClock(clientClock, read.getHRC());
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        UpdateResult result = (UpdateResult) updateResult;

        if (clientClock.get(result.getCloudletId()) > result.getCloudletCounter()) {
            throw new RuntimeException("Impossible scenario");
        }

        clientClock.put(result.getCloudletId(), result.getCloudletCounter());
    }

    public void migrationOver(Map<Long, Integer> cloudletClock) {
        migrationOver();
        this.clientClock = new HashMap<>(cloudletClock);
    }

}
