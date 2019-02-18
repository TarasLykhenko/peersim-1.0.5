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

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalStats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
    private static final String PAR_PARTITIONS_FILE = "partitions_file";
    private final static String PAR_TAKE_STATISTICS_EVERY = "statistics_window";
    private final static String PAR_WHEN_TO_PARTITION = "partition_start";
    private final static String PAR_WHEN_TO_UNPARTATITION = "partition_end";
    private final static String PAR_LEVELS = "levels";

    /**
     * The protocol to operate on.
     *
     * @config
     */
    private static final String PAR_PROT = "protocol";
    private static final String PAR_TYPE = "type";
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
    private final int pidType;
    private final PrintWriter writer;
    private final PrintWriter importantWriter;

    private final String partitionsFile;

    private final int takeStatisticsEvery;
    private final int targetCyclesToPartition;
    private final int targetCyclesToUnpartition;
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
        pidType = Configuration.getPid(name + "." + PAR_TYPE);

        iteration = 1;
        outputFile = Configuration.getString(name + "." + PAR_OUTPUT);
        partitionsFile = Configuration.getString(name + "." + PAR_PARTITIONS_FILE);
        TAKE_STATISTICS_EVERY = Configuration.getDouble(PAR_TAKE_STATISTICS_EVERY);
        double WHEN_TO_PARTITION = Configuration.getDouble(PAR_WHEN_TO_PARTITION);
        double WHEN_TO_UNPARTATITION = Configuration.getDouble(PAR_WHEN_TO_UNPARTATITION);
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
        targetCyclesToPartition = Math.round((float) (WHEN_TO_PARTITION / 100) * cycles);
        targetCyclesToUnpartition = Math.round((float) (WHEN_TO_UNPARTATITION / 100) * cycles);
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
        System.out.println("Iteration: " + iteration + " | time: " + CommonState.getTime());
        if (iteration == targetCyclesToPartition) {
            partitionConnections();
        }

        if (iteration != cycles) {
            if (iteration % takeStatisticsEvery != 0) {
                return false;
            }
        }
        System.out.println("Taking at " + iteration + " takeEvery is " + takeStatisticsEvery);

        int aggregateUpdates = 0;
        int aggregateReads = 0;
        int aggregateRemoteReads = 0;
        int aggregateProcessing = 0;
        int nNodes = 0;
        double aggregateFullMetadata = 0;
        double aggregatePartialMetadata = 0;
        long totalClients = 0;
        long totalSentMigrations = 0;
        long totalReceivedMigrations = 0;
        long totalPendingClients = 0;
        currentPoint += TAKE_STATISTICS_EVERY;
        print("Observer init ======================");
        print("CURRENT POINT: " + currentPoint);
        for (int i = 0; i < Network.size(); i++) {
            Node node = Network.get(i);
            StateTreeProtocolInstance v = (StateTreeProtocolInstance) Network.get(i).getProtocol(pid);
            TypeProtocol typeProt = (TypeProtocol) Network.get(i).getProtocol(pidType);
            if (typeProt.getType() == TypeProtocol.Type.DATACENTER) {
                nNodes++;
                print("[Node " + i + "]=========================");
                //System.out.println("Processed "+v.processedToString());
                print("Average processing time: " + v.getAverageProcessingTime());
                print("Average latency time: " + v.getLatencyProcessingTime());
                aggregateProcessing += v.getAverageProcessingTime();
                print("Total updates: " + v.getNumberUpdates());
                print("Total local reads: " + v.getNumberLocalReads());
                print("Total remote reads: " + v.getNumberRemoteReads());
                print("Total sent migrations: " + v.sentMigrations);
                print("Total received migrations: " + v.receivedMigrations);
                print("Clients: " + v.clients.size());
                print("Pending clients: " + v.pendingClientsQueue.size());
                aggregateUpdates += v.getNumberUpdates();
                aggregateReads += v.getNumberLocalReads();
                aggregateRemoteReads += v.getNumberRemoteReads();
                aggregateFullMetadata += v.getFullMetadata();
                aggregatePartialMetadata += v.getPartialMetadata();
                totalClients += v.clients.size();
                totalSentMigrations += v.sentMigrations;
                totalReceivedMigrations += v.receivedMigrations;
                totalPendingClients += v.pendingClientsQueue.size();
                //System.out.println(typeProt.getReplicationGroup(3));
                //System.out.println("Metadata "+v.getMetadataVector().toString());
                //System.out.println("Data "+v.getDataVector().toString());
                System.out.println();
                //writer.println(node.getID()+v.processedToStringFileFormat());

                /*
                for (int level = 0; level < levels; level++) {
                    Set<StateTreeProtocol> levelsToNodes = v.getNeighboursOfDatacenter(level);
                    print("Level " + level + ": " + levelsToNodes.stream()
                            .map(StateTreeProtocol::getNodeId)
                            .map(Object::toString)
                            .sorted()
                            .collect(Collectors.joining("-")));
                    for (DataObject dataObject : v.getDataObjectsFromLevel(level)) {
                        print("dataobject: " + dataObject.getDebugInfo());
                    }
                }
                */
            }
        }

        print("Average processing time(" + nNodes + "nodes): " + ((double) aggregateProcessing / (double) nNodes));
        print("Average metadata size for full repliction: " + aggregateFullMetadata / (double) nNodes);
        print("Average metadata size for partial repliction: " + aggregatePartialMetadata / (double) nNodes);
        int totalOps = aggregateUpdates + aggregateReads + aggregateRemoteReads;
        print("Total ops: " + totalOps);
        print("Total ops since last %: " + (totalOps - totalOpsPrevious));
        print("CURRENT POINT & Total ops since last %: " + currentPoint + " - " + (totalOps - totalOpsPrevious));
        printImportant("Total ops (%: " + currentPoint + ") - " + (totalOps - totalOpsPrevious));
        this.totalOpsPrevious = totalOps;
        print("% of reads: " + ((double) ((aggregateReads + aggregateRemoteReads) * 100) / (double) totalOps));
        print("% of updates: " + ((double) (aggregateUpdates * 100) / (double) totalOps));
        print("% of local reads: " + ((double) (aggregateReads * 100) / (double) totalOps));
        print("% of remote reads: " + ((double) (aggregateRemoteReads * 100) / (double) totalOps));
        print("% (remote reads/total reads): " + ((double) (aggregateRemoteReads * 100) / (double) (totalOps - aggregateUpdates)));
        print("Total clients: " + totalClients);
        print("Total sent Migrations: " + totalSentMigrations);
        print("Total received Migrations: " + totalReceivedMigrations);
        print("Pending clients: " + totalPendingClients);
        printImportant(totalPendingClients + "/" + (totalClients + totalPendingClients) + " pending clients");
        print("Total clients + pending clients: " + (totalClients + totalPendingClients));
        debugPercentages();
        print("Observer end =======================");
        print("");
        print("");
        /* Terminate if accuracy target is reached */

        if (cycles == iteration) {
            writer.close();
            importantWriter.close();
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

    private void partitionConnections() {
        System.out.println("Partitioning at iteration " + iteration + ", time " + CommonState.getTime());
        try (BufferedReader br = new BufferedReader(new FileReader(partitionsFile))) {
            String line = br.readLine();
            int counter = 0;
            while (line != null) {
                String[] partitions = line.split("\t");
                for (int i = 0; i < partitions.length; i++) {
                    int partition = Integer.valueOf(partitions[i]);
                    if (partition == 1) {
                        Node src = Network.get(counter);
                        Node dst = Network.get(i);

                        PointToPointTransport.partitionTable.get(src.getID())
                                .put(dst.getID(), targetCyclesToUnpartition * logTime);
                    }
                }
                line = br.readLine();
                counter++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

//--------------------------------------------------------------------------

