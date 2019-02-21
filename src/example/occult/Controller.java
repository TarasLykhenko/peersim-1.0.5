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

package example.occult;

import example.common.AbstractController;
import example.common.BasicClientInterface;

import java.io.IOException;
import java.util.Set;


public class Controller extends AbstractController {

//--------------------------------------------------------------------------
//Parameters
//--------------------------------------------------------------------------


//--------------------------------------------------------------------------
// Fields
//--------------------------------------------------------------------------

    private long totalStaleReadsPrevious;
    private long totalCatchAllPrevious;
    private long totalMasterMigrationsPrevious;


//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------

    public Controller(String name) throws IOException {
        super(name);
    }


    @Override
    public void doAdditionalExecution(Set<BasicClientInterface> clients) {

        /*
        protocol.writer.println("----- END RESULTS -----");
        for (Integer key : protocol.keysToCausalTimestamps.keySet()) {
            protocol.writer.println("Have key:" + key + "|v:" + protocol.keysToCausalTimestamps.get(key));
        }
        protocol.writer.close();
        */

        int aggregateStaleReads = 0;
        int aggregateMasterMigrations = 0;
        int aggregateCatchAllReads = 0;

        for (BasicClientInterface basicClient : clients) {
            OccultClientInterface client = (OccultClientInterface) basicClient;

            aggregateStaleReads += client.getNumberStaleReads();
            aggregateMasterMigrations += client.getNumberMasterMigrations();
            aggregateCatchAllReads += client.getNumberCatchAll();
        }

        printImportant("Total stale reads: " + (aggregateStaleReads - totalStaleReadsPrevious));
        printImportant("Total master migrations: " + (aggregateMasterMigrations - totalMasterMigrationsPrevious));
        printImportant("Total catchAll: " + (aggregateCatchAllReads - totalCatchAllPrevious));

        this.totalStaleReadsPrevious = aggregateStaleReads;
        this.totalMasterMigrationsPrevious = aggregateMasterMigrations;
        this.totalCatchAllPrevious = aggregateCatchAllReads;
    }

    public void doEndExecution(Set<BasicClientInterface> clients) {
        for (BasicClientInterface basicClient : clients) {
            OccultClientInterface client = (OccultClientInterface) basicClient;

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
                    + " | staleReads: " + client.getNumberStaleReads()
                    + " | CatchAll: " + client.getNumberCatchAll()
                    + extraString);
        }
    }
}
