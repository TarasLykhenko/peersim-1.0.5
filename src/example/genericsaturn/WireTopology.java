package example.genericsaturn;

import example.common.PointToPointTransport;
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
     * The alpha parameter. It affects the distance relevance in the wiring
     * process. Default value: 0.5.
     *
     * @config
     */
    private static final String PAR_ALPHA = "alpha";

    /**
     * The coordinate protocol to look at.
     *
     * @config
     */
    private static final String PAR_TYPE_PROT = "type_protocol";

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
    private final int typePid;

    private final String path, topology;

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
        typePid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
    }

    public void wire(Graph graph) {
        int clientRequestDelay = Configuration.getInt(PAR_CLIENT_REQUEST_DELAY);
        try (BufferedReader br = new BufferedReader(new FileReader(path + "/" + topology))) {
            String line = br.readLine();
            int source = 0;
            while (line != null) {
                String[] latencies = line.split("	");
                for (int target = 0; target < latencies.length; target++) {
                    int latency = Integer.parseInt(latencies[target]);
                    if (latency >= 0) {
                        graph.setEdge(source, target);

                        graph.setEdge(source, target);

                        PointToPointTransport.addLatency((long) source, (long) target, latency);
                    } else if (latency == -1) {
                        PointToPointTransport.addLatency((long) source, (long) target, clientRequestDelay);
                    }
                }
                line = br.readLine();
                source++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        GroupsManager.getInstance().populate(graph);
    }
}
