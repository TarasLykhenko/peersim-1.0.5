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

package example.genericsaturn;

import example.common.AbstractController;
import example.common.BasicClientInterface;
import example.common.BasicStateTreeProtocol;
import example.common.Settings;
import peersim.core.Network;
import peersim.util.IncrementalStats;

import java.io.IOException;
import java.util.Set;

/**
 * Print statistics over a vector. The vector is defined by a protocol,
 * specified by {#PAR_PROT}, that has to  implement
 * { SingleValue}.
 * Statistics printed are: min, max, number of samples, average, variance,
 * number of minimal instances, number of maximal instances (using
 * {@link IncrementalStats#toString}).
 *
 * @see IncrementalStats
 */
public class Controller extends AbstractController {


    /**
     * Standard constructor that reads the configuration parameters.
     * Invoked by the simulation engine.
     *
     * @param name the configuration prefix for this class
     */
    public Controller(String name) throws IOException {
        super(name, "saturn");
    }


    @Override
    public void doAdditionalExecution(Set<BasicClientInterface> clients) {
        for (int i = 0; i < Network.size(); i++) {
            BasicStateTreeProtocol v = (BasicStateTreeProtocol) Network.get(i).getProtocol(pid);
            for (BasicClientInterface client : v.getClients()) {
                if (Settings.PRINT_INFO) {
                    System.out.println("Node " + v.getNodeId() + " has client " + client.getId());
                }
            }
        }
    }
}

