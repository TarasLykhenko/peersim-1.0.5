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

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);
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

        this.clientPast = read.getHRC().merge(clientPast);
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        UpdateResult result = (UpdateResult) updateResult;

        //System.out.println("Storing: ");
        //result.getHRC().print();
        clientPast = new HRC(result.getHRC());
    }

    public void migrationOver(HRC clientTransformedHRC) {
        migrationOver();
        this.clientPast = new HRC(clientTransformedHRC);
    }

}
