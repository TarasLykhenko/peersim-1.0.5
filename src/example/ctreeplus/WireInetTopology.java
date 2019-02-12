/*
 * Copyright (c) 2003-2005 The BISON Project
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

package example.ctreeplus;

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
 * This class applies a HOT topology on a any {@link Linkable} implementing
 * protocol.
 * 
 * @author Gian Paolo Jesi
 */
public class WireInetTopology extends WireGraph {
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
    public WireInetTopology(String prefix) {
        super(prefix);
        path = Configuration.getString(prefix + "." + PAR_PATH, "example/topologies");
        topology = Configuration.getString(prefix + "." + PAR_TOPOLOGY, "default.top");
        typePid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
    }

    /**
     * Performs the actual wiring.
     * @param g a {@link peersim.graph.Graph} interface object to work on.
     */
    public void wire(Graph g) {
    	BufferedReader br = null;
        try {
        	br = new BufferedReader(new FileReader(path+"/"+topology));
            String line = br.readLine();
            int counter = 1;
            while (line != null) {
                String[] parts = line.split("	");
                for (int i=0; i<parts.length; i++) {
                	String[] ints = parts[i].split(",");
                	int latency = Integer.valueOf(ints[0]);
                	int frequency = Integer.valueOf(ints[1]);
                	if (latency >= 0) {	
                    	g.setEdge(counter, i);
                    	g.setEdge(i, counter);
                    	Node src = (Node) g.getNode(counter);
                    	Node dst = (Node) g.getNode(i);
                    	InetType srcType =(InetType) src.getProtocol(typePid);
                    	InetType dstType =(InetType) dst.getProtocol(typePid);
                    	srcType.setLatency(dst.getID(), latency);
                    	dstType.setLatency(src.getID(), latency);
                    	srcType.setFrequency(dst.getID(), frequency);
                    	dstType.setFrequency(src.getID(), frequency);
                	}
                }
                line = br.readLine();
                counter++;
            }
            br.close();
        } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
