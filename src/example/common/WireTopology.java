package example.common;

import peersim.config.Configuration;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static example.common.Settings.CLIENT_REQUEST_LATENCY;

/**
 * creates a undirected graph based on matrix (in text file)
 *
 * @author bravogestoso
 */

public class WireTopology extends WireGraph {

    /**
     * Standard constructor that reads the configuration parameters. Normally
     * invoked by the simulation engine.
     *
     * @param prefix the configuration prefix for this class
     */
    public WireTopology(String prefix) {
        super(prefix);
    }

    public void wire(Graph graph) {
        try (BufferedReader br = new BufferedReader(new FileReader(Settings.TOPOLOGY_FILE))) {
            String line = br.readLine();
            int source = 0;
            while (line != null) {
                String[] latencies = line.split("	");
                for (int target = 0; target < latencies.length; target++) {
                    int latency = Integer.parseInt(latencies[target]);

                    if (latency >= 0) {
                        graph.setEdge(source, target);
                        PointToPointTransport.addLatency((long) source, (long) target, latency);
                    } else if (latency == -1) {
                        PointToPointTransport.addLatency( (long) source, (long) target, CLIENT_REQUEST_LATENCY);
                    }
                }
                line = br.readLine();
                source++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
