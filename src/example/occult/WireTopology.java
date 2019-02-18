package example.occult;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * creates a undirected graph based on matrix (in text file)
 *
 * @author bravogestoso
 */

public class WireTopology extends WireGraph {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------

    /**
     * The coordinate protocol to look at.
     *
     * @config
     */

    private static final String PAR_PATH = "topology_path";

    private static final String PAR_TOPOLOGY = "topology_file";

    private static final String PAR_CLIENT_REQUEST_DELAY = "client_request_latency";


    // --------------------------------------------------------------------------
    // Fields
    // --------------------------------------------------------------------------
    /* A parameter that affects the distance importance. */

    /**
     * Coordinate protocol pid.
     */


    private final String path;
    private final String topology;

    // --------------------------------------------------------------------------
    // Initialization
    // --------------------------------------------------------------------------

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    public WireTopology(String prefix) {
        super(prefix);
        path = Configuration.getString(prefix + "." + PAR_PATH, "example/topologies");
        topology = Configuration.getString(prefix + "." + PAR_TOPOLOGY, "default.top");
    }

    public void wire(Graph graph) {
        int clientRequestDelay = Configuration.getInt(PAR_CLIENT_REQUEST_DELAY);
        try (BufferedReader br = new BufferedReader(new FileReader(path + "/" + topology))) {
            String line = br.readLine();
            int counter = 0;
            while (line != null) {
                String[] latencies = line.split("	");
                for (int i = 0; i < latencies.length; i++) {
                    int latency = Integer.parseInt(latencies[i]);

                    Node src = (Node) graph.getNode(counter);
                    Node dst = (Node) graph.getNode(i);

                    if (latency >= 0) {
                        graph.setEdge(counter, i);

                        PointToPointTransport.addLatency(src.getID(), dst.getID(), latency);
                    } else if (latency == -1) {
                        PointToPointTransport.addLatency(src.getID(), dst.getID(), clientRequestDelay);
                    }
                }
                line = br.readLine();
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
