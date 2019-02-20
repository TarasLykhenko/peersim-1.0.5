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

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalStats;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

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
public class Controller implements Control {

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
    private static final String PAR_ACCURACY = "accuracy";
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
    private final String name, outputFile;

    /**
     * Accuracy for standard deviation used to stop the simulation
     */
    private final double accuracy;

    private final double TAKE_STATISTICS_EVERY;

    /**
     * Protocol identifier
     */
    private final int pid;
    private PrintWriter writer;
    private PrintWriter importantWriter;
    private final boolean printImportant;
    private final boolean printVerbose;


    private final int takeStatisticsEvery;
    private final int levels;

    private int iteration;
    private int logTime;
    private int cycles;
    private int totalOpsPrevious;
    private long totalMigrationsPrevious;
    private long totalStaleReadsPrevious;
    private long totalCatchAllPrevious;
    private long totalMasterMigrationsPrevious;


//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------

    /**
     * Standard constructor that reads the configuration parameters.
     * Invoked by the simulation engine.
     *
     * @param name the configuration prefix for this class
     */
    public Controller(String name) throws IOException {
        this.name = name;
        accuracy = Configuration.getDouble(name + "." + PAR_ACCURACY, -1);
        pid = Configuration.getPid(name + "." + PAR_PROT);

        iteration = 1;
        outputFile = Configuration.getString(name + "." + PAR_OUTPUT);

        TAKE_STATISTICS_EVERY = Configuration.getDouble(PAR_TAKE_STATISTICS_EVERY);
        levels = Configuration.getInt(PAR_LEVELS);
        int endTime = Configuration.getInt("simulation.endtime");
        logTime = Configuration.getInt("simulation.logtime");
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

        takeStatisticsEvery = Math.round((float) (TAKE_STATISTICS_EVERY / 100) * cycles);
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
     * {@value #PAR_ACCURACY}, and the time of the simulation is larger then zero
     * (i.e. it has started).
     */
    private int currentPoint = 0;

    public boolean execute() {
        iteration++;

        if (iteration != cycles) {
            if (iteration % takeStatisticsEvery != 0) {
                return false;
            }
        }
        System.out.println("Taking at " + iteration + " takeEvery is " + takeStatisticsEvery);

        int aggregateUpdates = 0;
        int aggregateReads = 0;

        int nNodes = 0;
        long totalClients = 0;
        long totalMigrations = 0;
        long waitingClients = 0;
        long totalStaleReads = 0;
        long totalMasterMigrations = 0;
        long totalFalseShardReads = 0;
        currentPoint += TAKE_STATISTICS_EVERY;
        printImportant("Observer init ======================");
        print("CURRENT POINT: " + currentPoint);
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocol v = (StateTreeProtocol) Network.get(i).getProtocol(pid);
            for (ClientInterface client : v.getClients()) {
                totalClients++;
                aggregateReads += client.getNumberReads();
                aggregateUpdates += client.getNumberUpdates();
                totalMigrations += client.getNumberMigrations();
                totalMasterMigrations += client.getNumberMasterMigrations();
                if (client.isWaiting()) {
                    waitingClients++;
                }
                totalStaleReads += client.getNumberStaleReads();
                totalFalseShardReads += client.getNumberCatchAll();
            }
        }

        // printImportant("Total updates: " + aggregateUpdates);
        int totalOps = aggregateUpdates + aggregateReads ;
        print("Total ops: " + totalOps);
        print("Total ops since last %: " + (totalOps - totalOpsPrevious));
        print("CURRENT POINT & Total ops since last %: " + currentPoint + " - " + (totalOps - totalOpsPrevious));
        print("% of reads: " + ((double) ((aggregateReads) * 100) / (double) totalOps));
        print("% of updates: " + ((double) (aggregateUpdates * 100) / (double) totalOps));
        print("% of local reads: " + ((double) (aggregateReads * 100) / (double) totalOps));
        // print("% of remote reads: " + ((double) (aggregateRemoteReads * 100) / (double) totalOps));
        // print("% (remote reads/total reads): " + ((double) (aggregateRemoteReads * 100) / (double) (totalOps - aggregateUpdates)));
        print("Total clients: " + totalClients);
        print("Total Migrations: " + totalMigrations);
        printImportant(">> Total ops (%: " + currentPoint + ") - " + (totalOps - totalOpsPrevious));
        printImportant("Total Migrations: " + (totalMigrations - totalMigrationsPrevious));
        printImportant("Waiting clients: " + waitingClients);
        printImportant("Total stale reads: " + (totalStaleReads - totalStaleReadsPrevious));
        printImportant("Total master migrations: " + (totalMasterMigrations - totalMasterMigrationsPrevious));
        printImportant("Total catchAll: " + (totalFalseShardReads - totalCatchAllPrevious));
        printImportant("Observer end =======================");
        // debugPercentages();
        printImportant("");
        this.totalOpsPrevious = totalOps;
        this.totalMigrationsPrevious = totalMigrations;
        this.totalStaleReadsPrevious = totalStaleReads;
        this.totalMasterMigrationsPrevious = totalMasterMigrations;
        this.totalCatchAllPrevious = totalFalseShardReads;
        print("");
        /* Terminate if accuracy target is reached */

        if (cycles == iteration) {
            if (printVerbose) {
                writer.close();
            }
            if (printImportant) {
                importantWriter.close();
            }
            for (int i = 0; i < Network.size(); i++) {
                StateTreeProtocol protocol = (StateTreeProtocol) Network.get(i).getProtocol(pid);

                /*protocol.writer.println("----- END RESULTS -----");
                for (Integer key : protocol.keysToCausalTimestamps.keySet()) {
                    protocol.writer.println("Have key:" + key + "|v:" + protocol.keysToCausalTimestamps.get(key));
                }
                protocol.writer.close();
                */
                for (ClientInterface client : protocol.getClients()) {
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
            return true;
        }
        return false;
        /*return (stats.getStD()<=accuracy && CommonState.getTime()>0); */

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

    private void print(String string) {
        if (printVerbose) {
            writer.println(string);
        }
        if (WRITE_TO_SOUT) {
            System.out.println(string);
        }
    }

    private void printImportant(String string) {
        if (printImportant) {
            importantWriter.println(string);
        }
    }
}
