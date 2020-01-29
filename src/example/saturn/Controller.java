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

package example.saturn;

import example.common.AbstractController;
import example.common.BasicClientInterface;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalStats;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static example.common.Settings.CLIENT_READ_PERCENTAGE;

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

    public Controller(String name, String system) throws IOException {
        super(name, system);
    }

    @Override
    public void doEndExecution(Set<BasicClientInterface> clients) throws IOException {
        int totalReads = 0;
        int totalUpdates = 0;
        int totalMigrations = 0;
        float totalMigrationTime = 0;



        System.out.println("There are " + clients.size());
        for (BasicClientInterface c : clients) {
            Client client = (Client) c;
            totalReads += client.getNumberReads();
            totalUpdates += client.getNumberUpdates();
            totalMigrations += client.getNumberMigrations();
            totalMigrationTime += client.getAverageMigrationTime();


            String extraString = "";
            if (client.isWaiting()) {
                extraString = " | waitingSince: " + client.getWaitingSince();
            }
            System.out.println("Client " + client.getId()
                    + " | reads: " + client.getNumberReads()
                    + " | avgReadLat: " + client.getAverageReadLatency()
                    + " | updates: " + client.getNumberUpdates()
                    + " | avgUpdateLat: " + client.getAverageUpdateLatency()
                    + " | migrations: " + client.getNumberMigrations()

                    + extraString);
        }

        System.out.println("Average reads: " + ((float) totalReads / clients.size()));
        System.out.println("Average updates: " + ((float) totalUpdates / clients.size()));
        System.out.println("Average migrations: " + ((float) totalMigrations / clients.size()));
        System.out.println("Average migration time: " + (( totalMigrationTime / totalMigrations)));
        System.out.println("Total time migrating: " + totalMigrationTime);




    }

    @Override
    public void doAdditionalExecution(Set<BasicClientInterface> clients) {

    }
}

