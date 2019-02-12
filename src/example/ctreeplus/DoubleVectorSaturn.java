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

package example.ctreeplus;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Vector;

import peersim.core.Node;

/**
* The implementor class has a single parameter. This interface
* provides access to that parameter.
*/
public interface DoubleVectorSaturn {

/**
 * Returns
 * 			0: New and no matching data
 * 			1: New and matching data
 * 			2: Seen
 */
public int addMetadata(EventUID event);

/**
 * Returns
 * 			0: New and no matching metadata
 * 			1: New and matching metadata
 * 			2: Seen
 */
public int addData(EventUID event, Object data);

public void addQueueToQueue(PriorityQueue<EventUID> queue2, Long from);

public void addToPendingQueue(EventUID event, int epoch, long senderId);

public void addQueueToPendingQueue(PriorityQueue<EventUID> queue2, int epoch, long senderId);

public PriorityQueue<PendingEventUID> getPendingQueue(int epoch);

public void cleanPendingQueue(int epoch);

public void processQueue(PriorityQueue<EventUID> queue);

public int timestamp();

public int newEpoch();

public int getEpoch();

public int largerEpochSeen();

public void updateLargerEpochSeen(int newEpoch);

public void addProcessedEvent(EventUID event);

public void cleanProcessed();

public Vector<EventUID> getProcessed();

public String processedToString();

public String processedToStringFileFormat();

public void cloneQueue(HashMap<Long, PriorityQueue<EventUID>> queueInit);

public void clonePendingQueue(HashMap<Integer, PriorityQueue<PendingEventUID>> pendingQueueInit);

public void cloneProcessed(Vector<EventUID> processedInit);

public void cloneMetadata(VersionVector metadataInit);

public void cloneData(VersionVector dataInit);

public void addToQueue(EventUID event, Long from);

PriorityQueue<EventUID> getQueue(Long node);

void cleanQueue(Long node);

void processPendingEpoch(int epoch);

VersionVector getMetadataVector();

VersionVector getDataVector();

void initQueue(Node node);

}

