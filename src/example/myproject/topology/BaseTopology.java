package example.myproject.topology;

import peersim.config.Configuration;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

public abstract class BaseTopology extends WireGraph {

    private static final String PAR_DEBUG = "debug";

    private final boolean DEBUG;

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    BaseTopology(String prefix) {
        super(prefix);
        this.DEBUG = Configuration.getBoolean(PAR_DEBUG);
    }

    public void wire(Graph graph) {
        if (DEBUG) {
            for (int i = 0; i < graph.size(); i++) {
                debug(i + "'s neighbours: " + graph.getNeighbours(i));
            }
        }
    }

    protected void debug(String string) {
        if (DEBUG) {
            System.out.println(string);
        }
    }
}
