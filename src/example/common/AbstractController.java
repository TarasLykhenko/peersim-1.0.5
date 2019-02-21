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
    private static final String PAR_LEVELS = "levels";

    /**
     * The protocol to operate on.
     *
     * @config
     */
    private static final String PAR_TAKE_STATISTICS_EVERY = "statistics_window";
    private static final String PAR_PROT = "protocol";
    private static final String PAR_OUTPUT = "output_file";
    private static final String PAR_PRINT_IMPORTANT = "printImportant";
    private static final String PAR_PRINT_VERBOSE = "printVerbose";

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
    private final double takeStatisticsEveryPercentage;
    private PrintWriter writer;
    private PrintWriter importantWriter;
    private final boolean printImportant;
    private final boolean printVerbose;
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
    public AbstractController(String name) throws IOException {
        this.name = name;

        pid = Configuration.getPid(name + "." + PAR_PROT);
                outputFile = Configuration.getString(name + "." + PAR_OUTPUT);

        takeStatisticsEveryPercentage = Configuration.getDouble(PAR_TAKE_STATISTICS_EVERY);

        int endTime = Configuration.getInt("simulation.endtime");
        int logTime = Configuration.getInt("simulation.logtime");
        cycles = endTime / logTime;
        printImportant = Configuration.getBoolean(PAR_PRINT_IMPORTANT);
        printVerbose = Configuration.getBoolean(PAR_PRINT_VERBOSE);

        DateFormat dateFormat = new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss");
        Calendar cal = Calendar.getInstance();

        if (printVerbose) {
            String pathfile = outputFile + dateFormat.format(cal.getTime()) + ".txt";
            FileWriter fr = new FileWriter(pathfile, true);
            BufferedWriter br = new BufferedWriter(fr);
            writer = new PrintWriter(br);
        }

        if (printImportant) {
            String importantPathfile = outputFile + dateFormat.format(cal.getTime()) + "-2.txt";
            FileWriter fr2 = new FileWriter(importantPathfile, true);
            BufferedWriter br2 = new BufferedWriter(fr2);
            importantWriter = new PrintWriter(br2);
        }

        takeStatisticsEvery = Math.round((float)
                (takeStatisticsEveryPercentage / 100) * cycles);
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

        currentPoint += takeStatisticsEveryPercentage;
        Set<BasicClientInterface> clients = new HashSet<>();
        int aggregateUpdates = 0;
        int aggregateReads = 0;
        int aggregateMigrations = 0;
        int aggregateWaitingClients = 0;

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
        }
        int totalOps = aggregateUpdates + aggregateReads;

        printImportant("Observer init ======================");
        printImportant(">> Total ops (%: " + currentPoint + ") - " + (totalOps - totalOpsPrevious));
        printImportant("Total Migrations: " + (aggregateMigrations - totalMigrationsPrevious));
        printImportant("Waiting clients: " + aggregateWaitingClients);
        print("% of reads: " + ((double) ((aggregateReads) * 100) / (double) totalOps));
        print("% of updates: " + ((double) (aggregateUpdates * 100) / (double) totalOps));
        print("Total clients: " + totalClients);

        this.totalOpsPrevious = totalOps;
        this.totalMigrationsPrevious = aggregateMigrations;


        doAdditionalExecution(clients);

        printImportant("Observer end =======================");
        printImportant("");

        if (cycles == iteration) {
            if (printVerbose) {
                writer.close();
            }
            if (printImportant) {
                importantWriter.close();
            }
            doEndExecution(clients);
            return true;
        }
        return false;
    }

    public abstract void doAdditionalExecution(Set<BasicClientInterface> clients);

    public abstract void doEndExecution(Set<BasicClientInterface> clients);

    protected void print(String string) {
        if (printVerbose) {
            writer.println(string);
        }
        if (WRITE_TO_SOUT) {
            System.out.println(string);
        }
    }

    protected void printImportant(String string) {
        if (printImportant) {
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
