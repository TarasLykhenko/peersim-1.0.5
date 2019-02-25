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

package example.common;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.util.IncrementalStats;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import static example.common.Settings.PRINT_IMPORTANT;
import static example.common.Settings.PRINT_VERBOSE;
import static example.common.Settings.STATISTICS_WINDOW;

/**
 * Print statistics over a vector. The vector is defined by a protocol,
 * specified by {@value #PAR_PROT}, that has to  implement
 * { SingleValue}.
 * Statistics printed are: min, max, number of samples, average, variance,
 * number of minimal instances, number of maximal instances (using
 * {@link IncrementalStats#toString}).
 *
 * @see IncrementalStats
 */
public abstract class AbstractController implements Control {

//--------------------------------------------------------------------------
//Parameters
//--------------------------------------------------------------------------

    /**
     * The parameter used to determine the accuracy
     * (standard deviation) before stopping the simulation. If not
     * defined, a negative value is used which makes sure the observer
     * does not stop the simulation.
     *
     * @config
     * @see #execute
     */

    /**
     * The protocol to operate on.
     *
     * @config
     */
    private static final String PAR_PROT = "protocol";
    private static final String PAR_OUTPUT = "output_file";

    private static final boolean WRITE_TO_SOUT = true;

    // These are percentages


//--------------------------------------------------------------------------
// Fields
//--------------------------------------------------------------------------

    /**
     * The name of this observer in the configuration
     */
    private final String name;
    private final String outputFile;

    /**
     * Accuracy for standard deviation used to stop the simulation
     */

    /**
     * Protocol identifier
     */
    protected final int pid;
    private PrintWriter writer;
    private PrintWriter importantWriter;
    private int currentPoint = 0;


    private final int takeStatisticsEvery;

    private int iteration = 1;
    private int cycles;
    private int totalOpsPrevious;
    private long totalMigrationsPrevious;


//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------

    /**
     * Standard constructor that reads the configuration parameters.
     * Invoked by the simulation engine.
     *
     * @param name the configuration prefix for this class
     */
    public AbstractController(String name, String system) throws IOException {
        this.name = name;

        pid = Configuration.getPid(name + "." + PAR_PROT);
                outputFile = Configuration.getString(name + "." + PAR_OUTPUT);


        int endTime = Configuration.getInt("simulation.endtime");
        int logTime = Configuration.getInt("simulation.logtime");
        cycles = endTime / logTime;

        DateFormat dateFormat = new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss");
        Calendar cal = Calendar.getInstance();

        if (PRINT_VERBOSE) {
            String pathfile = outputFile + dateFormat.format(cal.getTime()) + ".txt";
            FileWriter fr = new FileWriter(pathfile, false);
            BufferedWriter br = new BufferedWriter(fr);
            writer = new PrintWriter(br);
        }

        if (PRINT_IMPORTANT) {
            String importantPathfile = outputFile + system + ".txt";
            FileWriter fr2 = new FileWriter(importantPathfile, false);
            BufferedWriter br2 = new BufferedWriter(fr2);
            importantWriter = new PrintWriter(br2);
        }

        takeStatisticsEvery = Math.round((STATISTICS_WINDOW / 100) * cycles);
    }


//--------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------

    /**
     * Print statistics over a vector. The vector is defined by a protocol,
     * specified by {@value #PAR_PROT}, that has to  implement
     * {SingleValue}.
     * Statistics printed are: min, max, number of samples, average, variance,
     * number of minimal instances, number of maximal instances (using
     * {@link IncrementalStats#toString}).
     *
     * @return true if the standard deviation is below the value of
     * {par accuracy (not defined)}, and the time of the simulation is larger then zero
     * (i.e. it has started).
     */
    public boolean execute() {
        iteration++;

        if (iteration != cycles) {
            if (iteration % takeStatisticsEvery != 0) {
                return false;
            }
        }

        currentPoint += STATISTICS_WINDOW;
        Set<BasicClientInterface> clients = new HashSet<>();
        int aggregateUpdates = 0;
        int aggregateReads = 0;
        int aggregateMigrations = 0;
        int aggregateWaitingClients = 0;
        int aggregateQueuedClients = 0;

        long totalClients = 0;

        for (int i = 0; i < Network.size(); i++) {
            BasicStateTreeProtocol v = (BasicStateTreeProtocol) Network.get(i).getProtocol(pid);
            for (BasicClientInterface client : v.getClients()) {

                totalClients++;
                aggregateReads += client.getNumberReads();
                aggregateUpdates += client.getNumberUpdates();
                aggregateMigrations += client.getNumberMigrations();
                if (client.isWaiting()) {
                    aggregateWaitingClients++;
                }
                clients.add(client);
            }

            aggregateQueuedClients += v.getQueuedClients();
        }
        int totalOps = aggregateUpdates + aggregateReads;

        printImportant("Observer init ======================");
        printImportant(">> Total ops (%: " + currentPoint + ") - " + (totalOps - totalOpsPrevious));
        printImportant("Total Migrations: " + (aggregateMigrations - totalMigrationsPrevious));
        printImportant("Waiting clients: " + aggregateWaitingClients);
        printImportant("Queued clients from migrations: " + aggregateQueuedClients);
        print("% of reads: " + ((double) ((aggregateReads) * 100) / (double) totalOps));
        print("% of updates: " + ((double) (aggregateUpdates * 100) / (double) totalOps));
        print("Total clients: " + totalClients);

        this.totalOpsPrevious = totalOps;
        this.totalMigrationsPrevious = aggregateMigrations;


        doAdditionalExecution(clients);

        printImportant("Observer end =======================");
        printImportant("");

        if (cycles == iteration) {
            if (PRINT_VERBOSE) {
                writer.close();
            }
            if (PRINT_IMPORTANT) {
                importantWriter.close();
            }
            doEndExecution(clients);
            return true;
        }
        return false;
    }

    public abstract void doAdditionalExecution(Set<BasicClientInterface> clients);

    public void doEndExecution(Set<BasicClientInterface> clients) {
        int totalReads = 0;
        int totalUpdates = 0;
        int totalMigrations = 0;
        int totalMigrationTime = 0;

        System.out.println("There are " + clients.size());
        for (BasicClientInterface client : clients) {

            totalReads += client.getNumberReads();
            totalUpdates += client.getNumberUpdates();
            totalMigrations += client.getNumberMigrations();
            totalMigrationTime += client.getAverageMigrationTime();

            String extraString = "";
            if (client.isWaiting()) {
                extraString = " | waitingSince: " + client.getWaitingSince();
            }
            System.out.println("Client " + client.getId()
                    + " locality: " + client.getLocality()
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
        System.out.println("Average migration time: " + ((float) totalMigrationTime / clients.size()));
        System.out.println("Total time migrating: " + totalMigrationTime);
    }

    protected void print(String string) {
        if (PRINT_VERBOSE) {
            writer.println(string);
        }
        if (WRITE_TO_SOUT) {
            System.out.println(string);
        }
    }

    protected void printImportant(String string) {
        if (PRINT_IMPORTANT) {
            importantWriter.println(string);
        }
    }

        /*
    private void debugPercentages() {
        int totalCount = 0;
        for (Map.Entry<Integer, Integer> entry : Client.levelsToCount.entrySet()) {
            totalCount += entry.getValue();
        }
        for (Map.Entry<Integer, Integer> entry : Client.levelsToCount.entrySet()) {
            print("% lvl " + entry.getKey() + ": " + ((double) (entry.getValue() * 100) / totalCount));
        }
    }
    */
}
