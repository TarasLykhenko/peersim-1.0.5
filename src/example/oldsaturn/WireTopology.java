package example.oldsaturn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import peersim.config.Configuration;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

/**
 * creates a undirected graph based on matrix (in text file)
 * @author bravogestoso
 *
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


    // --------------------------------------------------------------------------
    // Fields
    // --------------------------------------------------------------------------
    /* A parameter that affects the distance importance. */

    /** Coordinate protocol pid. */
    private final int typePid;

    private final String path, topology;

    // --------------------------------------------------------------------------
    // Initialization
    // --------------------------------------------------------------------------

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix
     *            the configuration prefix for this class
     */
    public WireTopology(String prefix) {
        super(prefix);
        path = Configuration.getString(prefix + "." + PAR_PATH, "example/topologies");
        topology = Configuration.getString(prefix + "." + PAR_TOPOLOGY, "default.top");
        typePid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
    }

    public void wire(Graph g) {
        try (BufferedReader br = new BufferedReader(new FileReader(path+"/"+topology))) {
            String line = br.readLine();
            int counter = 0;
            while (line != null) {
                String[] latencies = line.split("	");
                for (int i=0; i<latencies.length; i++) {
                    int latency = Integer.valueOf(latencies[i]);
                    if (latency >= 0) {
                        g.setEdge(counter, i);
                        //g.setEdge(i, counter);
                        Node src = (Node) g.getNode(counter);
                        Node dst = (Node) g.getNode(i);
                        TypeProtocol srcType =(TypeProtocol) src.getProtocol(typePid);
                        //InetType dstType =(InetType) dst.getProtocol(typePid);
                        srcType.setLatency(dst.getID(), latency);
                        //dstType.setLatency(src.getID(), latency);
                    }
                }
                line = br.readLine();
                counter++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
