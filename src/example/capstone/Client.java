package example.capstone;

import example.capstone.datatypes.ReadOperation;
import example.capstone.datatypes.ReadResult;
import example.capstone.datatypes.UpdateOperation;
import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static example.capstone.DatacenterProtocolInstance.*;

public class Client extends AbstractBaseClient {

    private Map<Integer, Integer> clientClock = new HashMap<>();
    private int counter;

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter, int locality) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality);
    }

    Map<Integer, Integer> getClientClock() {
        return new HashMap<>(clientClock);
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
        return new UpdateOperation(randomDataObject.getKey(), clientClock);
    }

    @Override
    public void handleReadResult(int key, Object readResult) {
        ReadResult read = (ReadResult) readResult;
        this.clientClock = getEntryWiseMaxClock(clientClock, read.getLocalDataClock());
    }

    // TODO
    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        //Integer integer
    }

}
