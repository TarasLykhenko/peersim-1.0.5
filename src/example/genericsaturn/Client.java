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

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);
    }

    public int timestamp() {
        return counter++;
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

    //TODO Ignorando este update metadata
    @Override
    public Operation specificDoUpdate(int updateLevel) {
        DataObject randomDataObject = chooseRandomDataObject(updateLevel);
        return new UpdateOperation(randomDataObject.getKey(),
                1,
                1,
                randomDataObject.getDebugInfo());
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
