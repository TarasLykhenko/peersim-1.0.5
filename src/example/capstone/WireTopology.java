package example.capstone;

import example.common.Settings;
import peersim.config.Configuration;
import peersim.dynamics.WireGraph;
import peersim.graph.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static example.common.PointToPointTransport.addLatency;
import static example.common.PointToPointTransport.staticGetLatency;
import static example.common.Settings.CLIENT_REQUEST_LATENCY;

/**
 * creates a undirected graph based on matrix (in text file)
 *
 * @author bravogestoso
 */

public class WireTopology extends WireGraph {

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
    }

    static Map<Integer, Long> brokerSources = new HashMap<>();

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

                        graph.setEdge(source, target);

                        addLatency((long) source, (long) target, latency);
                    } else if (latency == -1) {
                        addLatency((long) source, (long) target, CLIENT_REQUEST_LATENCY);
                    }
                }
                line = br.readLine();
                source++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Se level e 4, começa com 2
        int levels = Configuration.getInt("levels") - 2;
        int existingServers = Configuration.getInt("ndatanodes");
        int brokerIter = existingServers;
        int dcIter = 0;
        int numberItersFirstRound = (int) Math.pow(2, (double) levels);
        for (int i = 0; i < numberItersFirstRound; brokerIter++, i++) {
            if (dcIter == existingServers) {
                break;
            }
            long firstDC = dcIter++;
            long secndDC = dcIter++;
            int latency = staticGetLatency(firstDC, secndDC);

            graph.setEdge(brokerIter, (int) firstDC);
            graph.setEdge((int) firstDC, brokerIter);
            brokerSources.put(brokerIter, firstDC);

            addLatency((long) brokerIter, firstDC, 1);
            addLatency(firstDC, (long) brokerIter, 1);

            graph.setEdge(brokerIter, (int) secndDC);
            graph.setEdge((int) secndDC, brokerIter);

            addLatency((long) brokerIter, (long) secndDC, latency);
            addLatency((long) secndDC, (long) brokerIter, latency);
        }

        // Now connect brokers that connect brokers

        while (levels > 0) {
            int numberIter = (int) Math.pow(2, (double) levels - 1);
            int lastAddedBrokers = (int) Math.pow(2, (double) levels);
            int brokerStart = brokerIter - lastAddedBrokers;
            for (int i = 0; i < numberIter; brokerIter++, i++) {
                int offset = i * 2;

                int firstBroker = brokerStart + offset;
                int secondBroker = brokerStart + 1 + offset;
                long firstDC = brokerSources.get(firstBroker);
                long secondDC = brokerSources.get(secondBroker);

                graph.setEdge(brokerIter, firstBroker);
                graph.setEdge(firstBroker, brokerIter);
                graph.setEdge(brokerIter, secondBroker);
                graph.setEdge(secondBroker, brokerIter);

                int latency = staticGetLatency(firstDC, secondDC);
                addLatency((long) brokerIter, (long) firstBroker, 1);
                addLatency((long) firstBroker, (long) brokerIter, 1);
                addLatency((long) brokerIter, (long) secondBroker, latency);
                addLatency((long) secondBroker, (long) brokerIter, latency);
                brokerSources.put(brokerIter, firstDC);
            }
            levels--;
        }

        if (Settings.PRINT_INFO) {
            for (int i = 0; i < graph.size(); i++) {
                Collection<Integer> neighbours = graph.getNeighbours(i);
                System.out.println("Node " + i + " is connected to:");
                for (int neighbour : neighbours) {
                    int lat = staticGetLatency(i, neighbour);
                    System.out.println("   " + i + " to " + neighbour + ", lat: " + lat);
                }
            }
        }

        GroupsManager.getInstance().populate(graph);
    }
}
