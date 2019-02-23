package example.capstone.datatypes;

import example.common.datatypes.Operation;

import java.util.HashMap;
import java.util.Map;

public class UpdateOperation extends Operation {

    private Map<Integer, Integer> clientClock;

    public UpdateOperation(int key, Map<Integer, Integer> clientClock) {
        super(Type.UPDATE, key);
        this.clientClock = new HashMap<>(clientClock);
    }
}
