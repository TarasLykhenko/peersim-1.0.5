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
import peersim.config.IllegalParameterException;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

import java.util.HashMap;
import java.util.Map;


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

    /**
     * String name of the parameter used to configure the minimum latency.
     *
     * @config
     */
    private static final String PAR_MINDELAY = "mindelay";

    /**
     * String name of the parameter used to configure the maximum latency.
     * Defaults to {@value #PAR_MINDELAY}, which results in a constant delay.
     *
     * @config
     */
    private static final String PAR_MAXDELAY = "maxdelay";

    private static final String PAR_CLIENT_MIGRATION = "client_migration_latency";

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

    //---------------------------------------------------------------------
//Initialization
//---------------------------------------------------------------------
    private final int MIGRATION_REQUEST_LATENCY;

    /**
     * Reads configuration parameter.
     */
    public PointToPointTransport(String prefix) {
        min = Configuration.getLong(PAR_MINDELAY);
        max = Configuration.getLong(PAR_MAXDELAY);
        MIGRATION_REQUEST_LATENCY = Configuration.getInt(PAR_CLIENT_MIGRATION);
        if (max < min)
            throw new IllegalParameterException(prefix + "." + PAR_MAXDELAY,
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
    static Map<Long, Map<Long, Integer>> partitionClientTable = new HashMap<>();

    /**
     * Mapping is nodeSrc id to Map<NodeDst, PartitionEnd>
     */
    static Map<Long, Map<Long, Integer>> partitionDCTable = new HashMap<>();
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

    static void addLatency(Long src, Long dst, int latency) {
        latencies.computeIfAbsent(src, k -> new HashMap<>()).put(dst, latency);
    }

    /**
     * Returns a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    //TODO meter random latency
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
                delay = delay + 1;
            }

            lastWillBeReceived.get(srcId).put(destId, messageWillBeReceived);


            EDSimulator.add(delay, msg, dest, pid);
        }
    }

    private long addPartitionDelay(Object msg, Long srcId, Long destId, long delay) {
        int partitionOver;
        if (messageIsMigration(msg)) {
            partitionOver = partitionClientTable.get(srcId).get(destId);
        } else {
            partitionOver = partitionDCTable.get(srcId).get(destId);
        }

        if (partitionOver != 0) {
            partitionOver -= CommonState.getTime();
            if (partitionOver > 0) {
                System.out.println("ADDING DELAY!");
                delay += partitionOver;
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
     */
    private boolean messageIsMigration(Object msg) {
        return msg instanceof TreeProtocol.MigrationMessage;
    }
}