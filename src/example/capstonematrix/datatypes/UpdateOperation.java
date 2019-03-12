package example.capstonematrix.datatypes;

import example.common.datatypes.Operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateOperation extends Operation {

    private List<Map<Long, Integer>> clientHRC;

    public UpdateOperation(int key, List<Map<Long, Integer>> clientHRC) {
        super(Type.UPDATE, key);
        this.clientHRC = new ArrayList<>(clientHRC);
    }
}
