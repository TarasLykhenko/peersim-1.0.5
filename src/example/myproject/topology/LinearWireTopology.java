package example.myproject.topology;

import peersim.core.Network;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

public class LinearWireTopology extends BaseTopology {

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    public LinearWireTopology(String prefix) {
        super(prefix);
    }

    @Override
    public void wire(Graph graph) {
        for (int i = 0; i < Network.size() - 1; i++) {
            graph.setEdge(i, i + 1);
            graph.setEdge(i + 1, i);
        }

        super.wire(graph);
    }
}
