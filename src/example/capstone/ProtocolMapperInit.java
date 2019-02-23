package example.capstone;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

import java.util.HashMap;
import java.util.Map;

public class ProtocolMapperInit implements Control {

    static Map<Long, Type> nodeType = new HashMap<>();

    private int numberDatacenters;

    enum Type {
        BROKER,
        DATACENTER
    }

    public ProtocolMapperInit(String prefix) {
        numberDatacenters =  Configuration.getInt(prefix + "." + "ndatanodes");
    }

    @Override
    public boolean execute() {
        for (int i = 0; i < numberDatacenters; i++) {
            Node node = Network.get(i);
            nodeType.put(node.getID(), Type.DATACENTER);
        }
        for (int i = numberDatacenters; i < Network.size(); i++) {
            Node node = Network.get(i);
            nodeType.put(node.getID(), Type.BROKER);
        }

        return false;
    }
}
