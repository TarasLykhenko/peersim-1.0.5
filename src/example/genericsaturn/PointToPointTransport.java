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

    private static final String PAR_TYPE_PROT = "type_protocol";

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
    private final int typePid;

//---------------------------------------------------------------------
//Initialization
//---------------------------------------------------------------------

    /**
     * Reads configuration parameter.
     */
    public PointToPointTransport(String prefix) {
        typePid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
        min = Configuration.getLong(prefix + "." + PAR_MINDELAY);
        max = Configuration.getLong(prefix + "." + PAR_MAXDELAY, min);
        if (max < min)
            throw new IllegalParameterException(prefix + "." + PAR_MAXDELAY,
                    "The maximum latency cannot be smaller than the minimum latency");
        //range = max - min + 1;
        // Initializing tables
        for (long i = 0; i < Network.size(); i++) {
            partitionTable.put(i, new HashMap<>());
            lastWillBeReceived.put(i, new HashMap<>());
            for (long j = 0; j < Network.size(); j++) {
                partitionTable.get(i).put(j, 0);
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
     * Mapping is nodeSrc id to Map<NodeDst, PartitionEnd>
     */
    static Map<Long, Map<Long, Integer>> partitionTable = new HashMap<>();
    /**
     * Stores the time of when each message will be received. We want fifo, so each
     * new message should have a time higher than the lastWillBeReceived.
     *
     * Mapping is nodeSrc id to Map<NodeDst, lastTime>
     */
    private Map<Long, Map<Long, Long>> lastWillBeReceived = new HashMap<>();

//---------------------------------------------------------------------
//Methods
//---------------------------------------------------------------------

    /**
     * Delivers the message with a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    public void send(Node src, Node dest, Object msg, int pid) {
        // Check if the sender is in a partition to a given target
        // avoid calling nextLong if possible
        Long srcId = src.getID();
        Long destId = dest.getID();

        TypeProtocol srcType = (TypeProtocol) src.getProtocol(typePid);
        int latency = srcType.getLatency(dest.getID());
        if (latency != -1) {
            long delay = 0;
            if (latency != 0) {

                long extraDelay = CommonState.r.nextLong(max) - min;
                if (extraDelay < 0) {
                    extraDelay = 0;
                }

                delay = latency + extraDelay;
            }
            int partitionOver = 0;
            if (partitionTable.containsKey(srcId)) {
                Map<Long, Integer> partitionTargets = partitionTable.get(srcId);
                if (partitionTargets.containsKey(destId)) {
                    partitionOver = partitionTargets.get(destId);
                    partitionOver -= CommonState.getTime();
                    if (partitionOver > 0) {
                        delay += partitionOver;
                    }
                }
            }
            long messageWillBeReceived = CommonState.getTime() + delay;

            long lastWillBeReceivedDest = lastWillBeReceived.get(srcId).get(destId);
            if (messageWillBeReceived <= lastWillBeReceivedDest) {
                messageWillBeReceived = lastWillBeReceivedDest + 1;
                delay = delay + 1;
            }
            lastWillBeReceived.get(srcId).put(destId, messageWillBeReceived);

            /*
            if (partitionOver != 0) {
                System.out.println("UNDER PARTITION! : " + CommonState.getTime() + " | " + srcId + " sending msg at time " + messageWillBeReceived + " to " + destId);
                System.out.println("Delay is " + delay);
            }
            */

            EDSimulator.add(delay, msg, dest, pid);
        }
    }

    /**
     * Returns a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    public long getLatency(Node src, Node dest) {
        TypeProtocol srcType = (TypeProtocol) src.getProtocol(typePid);
        int latency = srcType.getLatency(dest.getID());
        if (latency > 0) {
            return latency;
            //return (range==1?latency+min:latency+min + CommonState.r.nextLong(range));
        } else {
            return 0;
        }
    }
}