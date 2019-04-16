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
import peersim.config.IllegalParameterException;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static example.common.Settings.MAX_DELAY;
import static example.common.Settings.MIN_DELAY;
import static example.common.Settings.PARTITION_START_PERCENTAGE;
import static example.common.Settings.PARTITION_STOP_PERCENTAGE;
import static example.common.Settings.SHOULD_PARTITION_CLIENTS;
import static example.common.Settings.SHOULD_PARTITION_DC;
import static example.common.Settings.PARTITIONS_ARE_DELAYS;
import static example.common.Settings.PARTITION_STRETCH_L1_PERCENTAGE;
import static example.common.Settings.PARTITION_STRETCH_L2_PERCENTAGE;
import static example.common.Settings.PARTITION_STRETCH_L3_PERCENTAGE;
import static example.common.Settings.PARTITION_MESSAGE_L1_AFFECTED_PERCENTAGE;
import static example.common.Settings.PARTITION_MESSAGE_L2_AFFECTED_PERCENTAGE;
import static example.common.Settings.PARTITION_MESSAGE_L3_AFFECTED_PERCENTAGE;


/**
 * Implement a transport layer that reliably delivers messages with a random
 * delay, that is drawn from the configured interval according to the uniform
 * distribution.
 *
 * @author Alberto Montresor
 * @version $Revision: 1.14 $
 */
public final class PointToPointTransport implements Transport {

//---------------------------------------------------------------------
//Parameters
//---------------------------------------------------------------------

    private static final String PAR_DURATION = "CYCLES";
    private static final String PAR_PARTITIONS_CLIENTS = "partitions_clients";

//---------------------------------------------------------------------
//Fields
//---------------------------------------------------------------------

    /**
     * Minimum delay for message sending
     */
    private final long min;

    /**
     * Difference between the max and min delay plus one. That is, max delay is
     * min+range-1.
     */
    private final long max;


    private final int timePartitionStart;
    private final int timePartitionOver;
    private GroupsManagerInterface groupsManager;

    //---------------------------------------------------------------------
//Initialization
//---------------------------------------------------------------------

    /**
     * Reads configuration parameter.
     */
    public PointToPointTransport(String prefix) {
        min = MIN_DELAY;
        max = MAX_DELAY;

        int duration = Configuration.getInt(PAR_DURATION);
        String partitionsDCFile = getDCPartitionsFile();
        String partitionsClientsFile = getClientPartitionsFile();
        System.out.println("Partitions file: " + partitionsDCFile);

        timePartitionStart = Math.round((float) (PARTITION_START_PERCENTAGE / 100) * duration);
        timePartitionOver = Math.round((float) (PARTITION_STOP_PERCENTAGE / 100) * duration);

        if (max < min)
            throw new IllegalParameterException(prefix + ".",
                    "The maximum latency cannot be smaller than the minimum latency");
        // Initializing tables
        for (long i = 0; i < Network.size(); i++) {
            partitionClientTable.put(i, new HashMap<>());
            partitionDCTable.put(i, new HashMap<>());
            lastWillBeReceived.put(i, new HashMap<>());
            for (long j = 0; j < Network.size(); j++) {
                partitionClientTable.get(i).put(j, 0);
                partitionDCTable.get(i).put(j, 0);
                lastWillBeReceived.get(i).put(j, 0L);
            }
        }

        if (SHOULD_PARTITION_DC) {
            if (Settings.PRINT_INFO) {
                System.out.println("ADDING PARTITION TO DC");
            }
            partitionConnections(partitionsDCFile, partitionDCTable, timePartitionOver);
        }
        if (SHOULD_PARTITION_CLIENTS) {
            if (Settings.PRINT_INFO) {
                System.out.println("ADDInG PARTITION TO CLIENT");
            }
            partitionConnections(partitionsClientsFile, partitionClientTable, timePartitionOver);
        }
    }

    public void setGroupsManager(GroupsManagerInterface groupsManager) {
        this.groupsManager = groupsManager;
    }

    private String getDCPartitionsFile() {
        String overlayType = Configuration.getString("overlaytype");
        if (overlayType.equals("tree")) {
            switch (Settings.DC_PARTITION_LEVEL) {
                case 1:
                    return "example/partitions/eight_nodes_seven_brokers_partition_lvl_1.top";
                case 2:
                    return "example/partitions/eight_nodes_seven_brokers_partition_lvl_2.top";
                case 3:
                    return "example/partitions/eight_nodes_seven_brokers_partition_lvl_3.top";
                default:
                    return "example/partitions/eight_nodes_seven_brokers_partition_custom.top";
            }
        } else if (overlayType.equals("direct")) {
            switch (Settings.DC_PARTITION_LEVEL) {
                case 1:
                    return "example/partitions/eight_nodes_partition_lvl_1.top";
                case 2:
                    return "example/partitions/eight_nodes_partition_lvl_2.top";
                case 3:
                    return "example/partitions/eight_nodes_partition_lvl_3.top";
                default:
                    return "example/partitions/eight_nodes_partition_custom.top";
            }
        } else {
            throw new RuntimeException("Unknown overlay type");
        }
    }

    private String getClientPartitionsFile() {
        switch (Settings.CLIENTS_PARTITION_LEVEL) {
            case 1:
                return "example/client-partitions/eight_nodes_partition_lvl_1.top";
            case 2:
                return "example/client-partitions/eight_nodes_partition_lvl_2.top";
            case 3:
                return "example/client-partitions/eight_nodes_partition_lvl_3.top";
            default:
                return "example/client-partitions/eight_nodes_partition_custom.top";
        }
    }

//---------------------------------------------------------------------

    /**
     * Returns <code>this</code>. This way only one instance exists in the system
     * that is linked from all the nodes. This is because this protocol has no
     * node specific state.
     */
    public Object clone() {
        return this;
    }

    /**
     * Mapping is NodeSrc id to Map<NodeDst, PartitionEnd> but for client migrations
     */
    private Map<Long, Map<Long, Integer>> partitionClientTable = new HashMap<>();

    /**
     * Mapping is nodeSrc id to Map<NodeDst, PartitionEnd>
     */
    private Map<Long, Map<Long, Integer>> partitionDCTable = new HashMap<>();
    /**
     * Stores the time of when each message will be received. We want fifo, so each
     * new message should have a time higher than the lastWillBeReceived.
     *
     * Mapping is nodeSrc id to Map<NodeDst, lastTime>
     */
    private Map<Long, Map<Long, Long>> lastWillBeReceived = new HashMap<>();

    /**
     * Mapping is nodeSrc id to Map(NodeDst id, latency)
     */
    private static Map<Long, Map<Long, Integer>> latencies = new HashMap<>();

    public static void addLatency(Long src, Long dst, int latency) {
        latencies.computeIfAbsent(src, k -> new HashMap<>()).put(dst, latency);
    }

    /**
     * Returns a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    public long getLatency(Node src, Node dest) {
        return latencies.get(src.getID()).get(dest.getID());
        //return (range==1?latency+min:latency+min + CommonState.r.nextLong(range));
    }

    public static int staticGetLatency(long srcId, long destId) {
        return latencies.get(srcId).get(destId);
    }

    //---------------------------------------------------------------------
    //Methods
    //---------------------------------------------------------------------

    /**
     * Delivers the message with a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    public void send(Node src, Node dest, Object msg, int pid) {
        // Check if the senderDC is in a partition to a given target
        // avoid calling nextLong if possible
        Long srcId = src.getID();
        Long destId = dest.getID();

        int latency = latencies.get(src.getID()).get(dest.getID());
        if (latency != -1) {

            long delay = addBellDelay(latency);

            delay = addPartitionDelay(msg, srcId, destId, delay);

            long messageWillBeReceived = CommonState.getTime() + delay;

            long lastWillBeReceivedDest = lastWillBeReceived.get(srcId).get(destId);
            if (messageWillBeReceived <= lastWillBeReceivedDest) {
                messageWillBeReceived = lastWillBeReceivedDest + 1;
                delay = messageWillBeReceived - CommonState.getTime();
            }

            lastWillBeReceived.get(srcId).put(destId, messageWillBeReceived);

            EDSimulator.add(delay, msg, dest, pid);
        }
    }

    private long addPartitionDelay(Object msg, Long srcId, Long destId, long delay) {
        long currentTime = CommonState.getTime();

        if (currentTime < timePartitionStart || currentTime > timePartitionOver) {
            return delay;
        }

        int partitionOver;
        if (messageIsMigration(msg)) {
            partitionOver = partitionClientTable.get(srcId).get(destId);
        } else {
            partitionOver = partitionDCTable.get(srcId).get(destId);
        }

        if (partitionOver != 0) {
            if (PARTITIONS_ARE_DELAYS) {
                float stretch;
                float target_percentage;

                switch (groupsManager.getLowestCommonLevel(srcId, destId)) {
                    case 1:
                        target_percentage = PARTITION_MESSAGE_L1_AFFECTED_PERCENTAGE;
                        stretch =  1 + (float) (PARTITION_STRETCH_L1_PERCENTAGE / 100);
                        break;
                    case 2:
                        target_percentage = PARTITION_MESSAGE_L2_AFFECTED_PERCENTAGE;
                        stretch =  1 + (float) (PARTITION_STRETCH_L2_PERCENTAGE / 100);
                        break;
                    case 3:
                        target_percentage = PARTITION_MESSAGE_L3_AFFECTED_PERCENTAGE;
                        stretch =  1 + (float) (PARTITION_STRETCH_L3_PERCENTAGE / 100);
                        break;
                    default:
                        target_percentage = 0;
                        stretch = 1;
                }
                if (CommonState.r.nextLong(100) < target_percentage) {
                    delay = Math.round(delay * stretch);
                }
            }
            else {
                partitionOver -= currentTime;
                if (partitionOver > 0) {
                    if (Settings.PRINT_INFO) {
                        System.out.println("ADDING DELAY TO " + msg);
                    }
                    delay += partitionOver;
                }
            }
        }
        return delay;
    }

    private long addBellDelay(int latency) {
        long delay = 0;

        long extraDelay = CommonState.r.nextLong(max);
        if (extraDelay < min) {
            extraDelay = min;
        }
        delay = latency + extraDelay;
        return delay;
    }

    /**
     * Migration messages are sent from the client to the target DC, so
     * we need them to be immune to partitions.
     *
     * This is not very typeSafe, but we need it to be like this because of Saturn
     */
    private boolean messageIsMigration(Object msg) {
        return msg.getClass().getSimpleName().equals("MigrationMessage");
    }

    private void partitionConnections(String partitionsFile,
                                      Map<Long, Map<Long, Integer>> table,
                                      int timePartitionIsOver) {
        try (BufferedReader br = new BufferedReader(new FileReader(partitionsFile))) {
            String line = br.readLine();
            long counter = 0;
            while (line != null) {
                String[] partitions = line.split("\t");
                for (int i = 0; i < partitions.length; i++) {
                    long partition = Integer.valueOf(partitions[i]);
                    if (partition == 1) {
                        table.get(counter).put((long) i, timePartitionIsOver);
                    }
                }
                line = br.readLine();
                counter++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}