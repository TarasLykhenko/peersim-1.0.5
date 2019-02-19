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

package example.occult.no_compression;

import example.occult.ClientInterface;
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
import java.util.Map;

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
    private static final boolean WRITE_TO_FILE = true;
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
    private final PrintWriter writer;
    private final PrintWriter importantWriter;


    private final int takeStatisticsEvery;
    private final int levels;

    private int iteration;
    private int logTime;
    private int cycles;
    private int totalOpsPrevious;


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

        DateFormat dateFormat = new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss");
        Calendar cal = Calendar.getInstance();

        String pathfile = outputFile + dateFormat.format(cal.getTime()) + ".txt";
        FileWriter fr = new FileWriter(pathfile, true);
        BufferedWriter br = new BufferedWriter(fr);
        writer = new PrintWriter(br);

        String importantPathfile = outputFile + dateFormat.format(cal.getTime()) + "-2.txt";
        FileWriter fr2 = new FileWriter(importantPathfile, true);
        BufferedWriter br2 = new BufferedWriter(fr2);
        importantWriter = new PrintWriter(br2);

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
        long pendingOperations = 0;
        long totalStaleReads = 0;
        currentPoint += TAKE_STATISTICS_EVERY;
        print("Observer init ======================");
        print("CURRENT POINT: " + currentPoint);
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocolInstance v = (StateTreeProtocolInstance) Network.get(i).getProtocol(pid);
            for (ClientInterface client : v.clients) {
                totalClients++;
                aggregateReads += client.getNumberReads();
                aggregateUpdates += client.getNumberUpdates();
                totalMigrations += client.getNumberMigrations();
                if (client.isWaiting()) {
                    waitingClients++;
                }
                totalStaleReads += client.getNumberStaleReads();
            }
        }

        printImportant("Total updates: " + aggregateUpdates);
        int totalOps = aggregateUpdates + aggregateReads ;
        print("Total ops: " + totalOps);
        print("Total ops since last %: " + (totalOps - totalOpsPrevious));
        print("CURRENT POINT & Total ops since last %: " + currentPoint + " - " + (totalOps - totalOpsPrevious));
        printImportant("Total ops (%: " + currentPoint + ") - " + (totalOps - totalOpsPrevious));
        this.totalOpsPrevious = totalOps;
        print("% of reads: " + ((double) ((aggregateReads) * 100) / (double) totalOps));
        print("% of updates: " + ((double) (aggregateUpdates * 100) / (double) totalOps));
        print("% of local reads: " + ((double) (aggregateReads * 100) / (double) totalOps));
       // print("% of remote reads: " + ((double) (aggregateRemoteReads * 100) / (double) totalOps));
       // print("% (remote reads/total reads): " + ((double) (aggregateRemoteReads * 100) / (double) (totalOps - aggregateUpdates)));
        print("Total clients: " + totalClients);
        print("Total Migrations: " + totalMigrations);
        printImportant("Total Migrations: " + totalMigrations);
        printImportant("Waiting clients: " + waitingClients);
        printImportant("Total stale reads: " + totalStaleReads);
        // debugPercentages();
        print("Observer end =======================");
        print("");
        print("");
        /* Terminate if accuracy target is reached */

        if (cycles == iteration) {
            writer.close();
            importantWriter.close();
            for (int i = 0; i < Network.size(); i++) {
                StateTreeProtocolInstance protocol = (StateTreeProtocolInstance) Network.get(i).getProtocol(pid);
                protocol.writer.println("----- END RESULTS -----");
                for (Integer key : protocol.keysToCausalTimestamps.keySet()) {
                    protocol.writer.println("Have key:" + key + "|v:" + protocol.keysToCausalTimestamps.get(key));
                }
                protocol.writer.close();
                for (ClientInterface client : protocol.clients) {
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
                            + extraString);
                }
            }
            return true;
        }
        return false;
        /*return (stats.getStD()<=accuracy && CommonState.getTime()>0); */

    }

    private void debugPercentages() {
        int totalCount = 0;
        for (Map.Entry<Integer, Integer> entry : Client.levelsToCount.entrySet()) {
            totalCount += entry.getValue();
        }
        for (Map.Entry<Integer, Integer> entry : Client.levelsToCount.entrySet()) {
            print("% lvl " + entry.getKey() + ": " + ((double) (entry.getValue() * 100) / totalCount));
        }

    }

    private void print(String string) {
        if (WRITE_TO_FILE) {
            writer.println(string);
        }
        if (WRITE_TO_SOUT) {
            System.out.println(string);
        }
    }

    private void printImportant(String string) {
        System.out.println("WRITING!");
        importantWriter.println(string);
    }
}
