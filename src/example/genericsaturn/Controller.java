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
        super(name);
    }


    @Override
    public void doAdditionalExecution(Set<BasicClientInterface> clients) {

    }

    @Override
    public void doEndExecution(Set<BasicClientInterface> clients) {
        int totalReads = 0;
        int totalUpdates = 0;

        for (BasicClientInterface client : clients) {

            totalReads += client.getNumberReads();
            totalUpdates += client.getNumberUpdates();

            String extraString = "";
            if (client.isWaiting()) {
                extraString = " | waitingSince: " + client.getWaitingSince();
            }
            System.out.println("Client " + client.getId()
                    + " locality: " + client.getLocality()
                    + " | reads: " + client.getNumberReads()
                    + " | avgReadLat: " + client.getAverageReadLatency()
                    + " | updates: " + client.getNumberReads()
                    + " | avgUpdateLat: " + client.getAverageUpdateLatency()
                    + " | migrations: " + client.getNumberMigrations()
                    + extraString);
        }

        System.out.println("Average reads: " + ((float) totalReads / clients.size()));
        System.out.println("Average updates: " + ((float) totalUpdates / clients.size()));
    }
}

