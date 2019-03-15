package example.capstonematrix.datatypes;

import example.common.datatypes.Operation;

public class UpdateOperation extends Operation {

    private HRC clientHRC;

    public UpdateOperation(int key, HRC clientHRC) {
        super(Type.UPDATE, key);
        this.clientHRC = new HRC(clientHRC);
    }
}
