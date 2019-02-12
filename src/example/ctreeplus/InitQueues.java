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
public class InitQueues implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------
    /**
     * The protocol to operate on.
     * 
     * @config
     */
	private static final String PAR_ED_PROT = "ed_protocol";


    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    /** Protocol identifier, obtained from config property {@link #PAR_PROT}. */
    private final int ed;


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
    public InitQueues(String prefix) {
        ed = Configuration.getPid(prefix + "." + PAR_ED_PROT);
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
        DataED edProt = null;
        for (int i = 0; i < Network.size(); i++) {
            n = Network.get(i);
            edProt = (DataED) n.getProtocol(ed);
            edProt.initQueue(n);
        }
        return false;
    }

}
