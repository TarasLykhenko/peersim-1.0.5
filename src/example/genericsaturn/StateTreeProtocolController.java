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

import example.genericsaturn.datatypes.DataObject;
import peersim.config.Configuration;
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
import java.util.Set;
import java.util.stream.Collectors;

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
public class StateTreeProtocolController implements Control {


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
    private final static String PAR_WHEN_TO_PARTITION = "when_to_partition";
    private final static String PAR_WHEN_TO_UNPARTATITION = "when_to_unpartition";
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
    public StateTreeProtocolController(String name) {
        this.name = name;
        accuracy = Configuration.getDouble(name + "." + PAR_ACCURACY, -1);
        pid = Configuration.getPid(name + "." + PAR_PROT);
        pidType = Configuration.getPid(name + "." + PAR_TYPE);

        iteration = 1;
        outputFile = Configuration.getString(name + "." + PAR_OUTPUT);
        partitionsFile = Configuration.getString(name + "." + PAR_PARTITIONS_FILE);
        TAKE_STATISTICS_EVERY = Configuration.getDouble(name + "." + PAR_TAKE_STATISTICS_EVERY);
        double WHEN_TO_PARTITION = Configuration.getDouble(name + "." + PAR_WHEN_TO_PARTITION);
        double WHEN_TO_UNPARTATITION = Configuration.getDouble(name + "." + PAR_WHEN_TO_UNPARTATITION);
        levels = Configuration.getInt(PAR_LEVELS);
        int endTime = Configuration.getInt("simulation.endtime");
        logTime = Configuration.getInt("simulation.logtime");
        cycles = endTime / logTime;

        DateFormat dateFormat = new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss");
        Calendar cal = Calendar.getInstance();
        String pathfile = outputFile + dateFormat.format(cal.getTime()) + ".txt";
        FileWriter fr = null;
        try {
            fr = new FileWriter(pathfile, true);
        } catch (IOException e) {
            System.exit(1);
        }
        BufferedWriter br = new BufferedWriter(fr);
        writer = new PrintWriter(br);

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
        //TODO tirar comment
        //downServer();
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
                        print("dataobject: " + dataObject.getUniqueId());
                    }
                }
                */
            }
        }

        print("Average processing time(" + nNodes + "nodes): " + (new Double(aggregateProcessing) / new Double(nNodes)));
        print("Average metadata size for full repliction: " + aggregateFullMetadata / new Double(nNodes));
        print("Average metadata size for partial repliction: " + aggregatePartialMetadata / new Double(nNodes));
        int totalOps = aggregateUpdates + aggregateReads + aggregateRemoteReads;
        print("Total ops: " + totalOps);
        print("Total ops since last %: " + (totalOps - totalOpsPrevious));
        print("CURRENT POINT & Total ops since last %: " + currentPoint + " - " + (totalOps - totalOpsPrevious));
        this.totalOpsPrevious = totalOps;
        print("% of updates: " + (new Double(aggregateUpdates * 100) / new Double(totalOps)));
        print("% of local reads: " + (new Double(aggregateReads * 100) / new Double(totalOps)));
        print("% of remote reads: " + (new Double(aggregateRemoteReads * 100) / new Double(totalOps)));
        print("% (remote reads/total reads): " + (new Double(aggregateRemoteReads * 100) / new Double(totalOps - aggregateUpdates)));
        print("Total clients: " + totalClients);
        print("Total sent Migrations: " + totalSentMigrations);
        print("Total received Migrations: " + totalReceivedMigrations);
        print("Pending clients: " + totalPendingClients);
        print("Total clients + pending clients: " + (totalClients + totalPendingClients));
        print("Observer end =======================");
        print("");
        print("");
        /* Terminate if accuracy target is reached */

        if (cycles == iteration) {
            writer.close();
            return true;
        }
        return false;
        /*return (stats.getStD()<=accuracy && CommonState.getTime()>0); */

    }

    private void print(String string) {
        if (WRITE_TO_FILE) {
            writer.println(string);
        }
        if (WRITE_TO_SOUT) {
            System.out.println(string);
        }
    }

    private boolean hasPartitioned = false;
    private boolean hasUnpartitioned = false;

    private void downServer() {
        if (hasUnpartitioned) {
            return;
        }
        if (!hasPartitioned) {
            if (iteration != targetCyclesToPartition) {
                return;
            }
            partitionConnections(true);
            hasPartitioned = true;
        } else {
            if (iteration != targetCyclesToUnpartition) {
                return;
            }

            partitionConnections(false);
            //System.out.println("it: " + iteration + " | Unpartitioned DC " + partitionedNode);
            //System.out.println("cycles = " + cycles);
            hasUnpartitioned = true;
        }
    }

    public void partitionConnections(boolean toPartition) {
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

                        TreeProtocol srcTree = (TreeProtocol) src.getProtocol(pid);
                        TreeProtocol dstTree = (TreeProtocol) dst.getProtocol(pid);
                        if (toPartition) {
                            srcTree.setPartitionTarget(dst);
                            dstTree.setPartitionTarget(src);
                        } else {
                            srcTree.unpartitionTarget(dst);
                            dstTree.unpartitionTarget(src);
                        }

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

