package example.myproject.topology;

import example.myproject.Utils;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

public abstract class BaseTopology extends WireGraph {

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    BaseTopology(String prefix) {
        super(prefix);
    }

    public void wire(Graph graph) {
        if (Utils.DEBUG) {
            for (int i = 0; i < graph.size(); i++) {
                debug(i + "'s neighbours: " + graph.getNeighbours(i));
            }
        }
    }

    protected void debug(String string) {
        if (Utils.DEBUG) {
            System.out.println(string);
        }
    }
}
