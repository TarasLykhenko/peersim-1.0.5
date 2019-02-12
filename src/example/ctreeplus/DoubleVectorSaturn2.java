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
import java.util.Vector;

import peersim.core.Node;

/**
* The implementor class has a single parameter. This interface
* provides access to that parameter.
*/
public interface DoubleVectorSaturn2 {

/**
 * Returns
 * 			0: New and no matching data
 * 			1: New and matching data
 * 			2: Seen
 */
public void addMetadata(EventUID2 event);

/**
 * Returns
 * 			0: New and no matching metadata
 * 			1: New and matching metadata
 * 			2: Seen
 */
public void addData(EventUID2 event, Object data);

public void addQueueToQueue(Vector<EventUID2> queue2, Long from);

public void addToPendingQueue(EventUID2 event, int epoch, long senderId);

public void addQueueToPendingQueue(Vector<EventUID2> queue2, int epoch, long senderId);

public Vector<PendingEventUID2> getPendingQueue(int epoch);

public void cleanPendingQueue(int epoch);

public void processQueue(Vector<EventUID2> queue, long id);

public int timestamp();

public int newEpoch();

public int getEpoch();

public int largerEpochSeen();

public void updateLargerEpochSeen(int newEpoch);

public void addProcessedEvent(EventUID2 event);

public void cleanProcessed();

public Vector<EventUID2> getProcessed();

public String processedToString();

public String processedToStringFileFormat();

public void cloneQueue(HashMap<Long, Vector<EventUID2>> queueInit);

public void clonePendingQueue(HashMap<Integer, Vector<PendingEventUID2>> pendingQueueInit);

public void cloneProcessed(Vector<EventUID2> processedInit);

public void cloneMetadata(VersionVector metadataInit);

public void cloneData(VersionVector dataInit);

public void addToQueue(EventUID2 event, Long from);

Vector<EventUID2> getQueue(Long node);

void cleanQueue(Long node);

void processPendingEpoch(int epoch);

VersionVector getMetadataVector();

VersionVector getDataVector();

void initQueue(Node node);

public double getAverageProcessingTime();

}

