package example.saturn;

import example.common.AbstractBaseClient;
import example.common.datatypes.DataObject;
import example.common.datatypes.Operation;
import example.saturn.datatypes.ReadOperation;
import example.saturn.datatypes.UpdateOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Client extends AbstractBaseClient {

    /**
     * COPS Client state : A map of keys to version.
     * Every time a client reads, he adds the key and version on the map.
     * Every time a client writes, he uses the current context on the write, clears it
     * and adds the key and version of the write to the context
     */
    private Map<Integer, Integer> context = new HashMap<>();

    Map<Integer, Integer> getCopyOfContext() {
        return new HashMap<>(context);
    }

    public Client(int id, boolean isEager, Map<Integer, Set<DataObject>> dataObjectsPerLevel, StateTreeProtocol datacenter,
                  int locality, GroupsManager groupsManager) {
        super(id, isEager, dataObjectsPerLevel, datacenter, locality, groupsManager);
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
        return new UpdateOperation(dataObject.getKey(), context);
    }

    //------------------------------------------
    // ------------ SERVER RESPONSES -----------
    //------------------------------------------

    @Override
    public void handleReadResult(int key, Object readResult) {
        int version = (int) readResult;
        context.put(key, version);
    }

    @Override
    public void handleUpdateResult(int key, Object updateResult) {
        int version = (int) updateResult;
        context.put(key, version);
    }

}
