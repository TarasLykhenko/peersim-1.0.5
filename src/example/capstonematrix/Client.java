package example.capstonematrix;

import example.capstonematrix.datatypes.HRC;
import example.capstonematrix.datatypes.ReadOperation;
import example.capstonematrix.datatypes.ReadResult;
import example.capstonematrix.datatypes.UpdateOperation;
import example.capstonematrix.datatypes.UpdateResult;
import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;

import java.util.Map;
import java.util.Set;

public class Client extends AbstractBaseClient {

    private HRC clientPast;

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality, GroupsManager groupsManager) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality, groupsManager);
        int regionId = GroupsManager.getInstance().getMostSpecificRegion(datacenter.getNodeId());
        clientPast = new HRC(regionId);
    }

    HRC getClientHRC() {
        return new HRC(clientPast);
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
        return new UpdateOperation(dataObject.getKey(), clientPast);
    }

    @Override
    public void handleReadResult(int key, Object readResult) {
        ReadResult read = (ReadResult) readResult;

        this.clientPast = read.getHRC().merge(clientPast);
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        UpdateResult result = (UpdateResult) updateResult;

        //System.out.println("Storing: ");
        //result.getHRC().print();
        clientPast = new HRC(result.getHRC());
    }

    public void migrationOver(long dcId, HRC clientTransformedHRC) {
        migrationOver(dcId);
        this.clientPast = new HRC(clientTransformedHRC);
    }

}
