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

package example.myproject;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

import java.util.HashMap;
import java.util.Map;

import static example.common.Settings.MAX_DELAY;
import static example.common.Settings.MIN_DELAY;


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
//Fields
//---------------------------------------------------------------------

    /**
     * Constant minimum delay for message sending
     */
    private final long latency;


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

    /**
     * Reads configuration parameter.
     */
    public PointToPointTransport(String prefix) {
        min = MIN_DELAY;
        max = MAX_DELAY;
        latency = Configuration.getLong("latency");
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
     * Stores the time of when each message will be received. We want fifo, so each
     * new message should have a time higher than the lastWillBeReceived.
     *
     * Mapping is nodeSrc id to Map<NodeDst, lastTime>
     */
    private Map<Long, Map<Long, Long>> lastWillBeReceived = new HashMap<>();

    /**
     * Returns a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    //TODO meter random latency
    public long getLatency(Node src, Node dest) {
        return latency;
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

        long delay = addBellDelay(getLatency(src, dest));

        long messageWillBeReceived = CommonState.getTime() + delay;

        long lastWillBeReceivedDest = lastWillBeReceived
                .computeIfAbsent(srcId, k -> new HashMap<>())
                .getOrDefault(destId, 0L);

        if (messageWillBeReceived <= lastWillBeReceivedDest) {
            messageWillBeReceived = lastWillBeReceivedDest + 1;
            delay = messageWillBeReceived - CommonState.getTime();
        }

        lastWillBeReceived.get(srcId).put(destId, messageWillBeReceived);

        EDSimulator.add(delay, msg, dest, pid);
    }

    private long addBellDelay(long latency) {
        long delay = 0;

        long extraDelay = CommonState.r.nextLong(max);
        if (extraDelay < min) {
            extraDelay = min;
        }
        delay = latency + extraDelay;
        return delay;
    }
}