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

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

/**
 * <p>
 * This initialization class collects the simulation parameters from the config
 * file and generates uniformly random 2D-coordinates for each node. The
 * coordinates are distributed on a unit (1.0) square.
 * </p>
 * <p>
 * The first node in the {@link Network} is considered as the root node and its
 * coordinate is set to the center of the square.
 * </p>
 * 
 * 
 * @author Gian Paolo Jesi
 */
public class InetInitializer implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------
    /**
     * The protocol to operate on.
     * 
     * @config
     */
	private static final String PAR_TYPE_PROT = "type_protocol";
    private static final String PAR_NDATANODES = "ndatanodes";


    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    /** Protocol identifier, obtained from config property {@link #PAR_PROT}. */
    private final int pid;
    private final int n_datanodes;


    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    /**
     * Standard constructor that reads the configuration parameters. Invoked by
     * the simulation engine.
     * 
     * @param prefix
     *            the configuration prefix for this class.
     */
    public InetInitializer(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
        n_datanodes =  Configuration.getInt(prefix + "." + PAR_NDATANODES);
    }

    // ------------------------------------------------------------------------
    // Methods
    // ------------------------------------------------------------------------
    /**
     * Initialize the node coordinates. The first node in the {@link Network} is
     * the root node by default and it is located in the middle (the center of
     * the square) of the surface area.
     */
    public boolean execute() {
        Node n = null;
        InetType prot = null;
        DataED edProt = null;
        for (int i = 0; i < n_datanodes; i++) {
            n = Network.get(i);
            prot = (InetType) n.getProtocol(pid);
            prot.setType(0);
        }
        for (int i = n_datanodes; i < Network.size(); i++) {
            n = Network.get(i);
            prot = (InetType) n.getProtocol(pid);
            prot.setType(1);
        }
        return false;
    }

}
