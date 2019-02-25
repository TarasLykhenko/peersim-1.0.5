package example.capstone.datatypes;

import example.common.datatypes.Operation;

import java.util.HashMap;
import java.util.Map;

public class UpdateOperation extends Operation {

    private Map<Long, Integer> clientClock;

    public UpdateOperation(int key, Map<Long, Integer> clientClock) {
        super(Type.UPDATE, key);
        this.clientClock = new HashMap<>(clientClock);
    }
}
