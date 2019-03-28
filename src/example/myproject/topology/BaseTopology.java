package example.myproject.topology;

import example.myproject.Utils;
import example.myproject.datatypes.AssertException;
import peersim.config.Configuration;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

public class BaseTopology extends WireGraph {

    private TopologyInterface topology;

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    public BaseTopology(String prefix) {
        super(prefix);

        String topologyType = Configuration.getString("topology-type");

        switch (topologyType) {
            case "file":
                this.topology = new FileWireTopology();
                break;
            case "tree":
                this.topology = new TreeWireTopology();
                break;
            case "linear":
                this.topology = new LinearWireTopology();
                break;
            default:
                throw new AssertException("Unknown topology type: " + topologyType);
        }
    }

    public void wire(Graph graph) {
        topology.wire(graph);
        if (Utils.DEBUG) {
            for (int i = 0; i < graph.size(); i++) {
                debug(i + "'s neighbours: " + graph.getNeighbours(i));
            }
        }
    }

    protected static void debug(String string) {
        if (Utils.DEBUG) {
            System.out.println(string);
        }
    }
}
