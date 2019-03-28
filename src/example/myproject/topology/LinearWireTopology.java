package example.myproject.topology;

import peersim.core.Network;
import peersim.graph.Graph;

public class LinearWireTopology implements TopologyInterface {

    @Override
    public void wire(Graph graph) {
        for (int i = 0; i < Network.size() - 1; i++) {
            graph.setEdge(i, i + 1);
            graph.setEdge(i + 1, i);
        }
    }
}
