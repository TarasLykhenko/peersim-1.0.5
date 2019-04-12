package example.genericsaturn;

import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;
import example.genericsaturn.datatypes.ReadOperation;
import example.genericsaturn.datatypes.UpdateOperation;

import java.util.Map;
import java.util.Set;

public class Client extends AbstractBaseClient {

    private int counter;

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality, GroupsManager groupsManager) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality, groupsManager);
    }

    public int timestamp() {
        return counter++;
    }

    @Override
    public boolean doExtraBehaviour() {
        return false;
    }

    @Override
    public Operation specificDoRead(DataObject dataObject) {
        return new ReadOperation(dataObject.getKey());
    }

    //TODO Ignorando este update metadata
    @Override
    public Operation specificDoUpdate(DataObject dataObject) {
        return new UpdateOperation(dataObject.getKey(),
                1,
                1,
                dataObject.getDebugInfo());
    }

    @Override
    public void handleReadResult(int key, Object readResult) {
        // Nothing to do
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        // Nothing to do
    }

}
