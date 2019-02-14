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
import example.genericsaturn.datatypes.EventUID;
import example.genericsaturn.datatypes.PendingEventUID;
import example.genericsaturn.datatypes.VersionVector;
import peersim.core.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * The implementor class has a single parameter. This interface
 * provides access to that parameter.
 */
public interface StateTreeProtocol {

    /**
     * Returns
     * 0: New and no matching data
     * 1: New and matching data
     * 2: Seen
     */
    void addMetadata(EventUID event);

    /**
     * Returns
     * 0: New and no matching metadata
     * 1: New and matching metadata
     * 2: Seen
     */
    void addData(EventUID event, Object data);

    void addQueueToQueue(Vector<EventUID> queue2, Long from);

    void addToPendingQueue(EventUID event, int epoch, long senderId);

    void addQueueToPendingQueue(Vector<EventUID> queue2, int epoch, long senderId);

    Vector<PendingEventUID> getPendingQueue(int epoch);

    void cleanPendingQueue(int epoch);

    void processQueue(Vector<EventUID> queue, long id);

    int timestamp();

    int newEpoch();

    int getEpoch();

    int largerEpochSeen();

    void updateLargerEpochSeen(int newEpoch);

    void addProcessedEvent(EventUID event);

    void cleanProcessed();

    Vector<EventUID> getProcessed();

    String processedToString();

    String processedToStringFileFormat();

    void cloneQueue(Map<Long, Vector<EventUID>> queueInit);

    void clonePendingQueue(Map<Integer, Vector<PendingEventUID>> pendingQueueInit);

    void addToQueue(EventUID event, Long from);

    Vector<EventUID> getQueue(Long node);

    void cleanQueue(Long node);

    void processPendingEpoch(int epoch);

    VersionVector getMetadataVector();

    VersionVector getDataVector();

    void initQueue(Node node);

    double getAverageProcessingTime();
    //--------------------------------------------------------------------------
    //Replication groups methods
    //--------------------------------------------------------------------------

    boolean isInterested(long node, long key);

    //--------------------------------------------------------------------------
    //Client methods
    //--------------------------------------------------------------------------

    void setClients(Set<Client> clientList);

    void setClientsCycle(int clientsCycle);

    int getClientsCycle();

    //--------------------------------------------------------------------------
    //Statistics
    //--------------------------------------------------------------------------

    void cloneStatistics(int localReads, int remoteReads, int updates, int full, int partial);

    void incrementUpdates();

    void incrementRemoteReads();

    void incrementLocalReads();

    int getNumberUpdates();

    int getNumberRemoteReads();

    int getNumberLocalReads();

    void addFullMetadata(int update);

    void addPartialMetadata(int update);

    double getFullMetadata();

    double getPartialMetadata();

    //--------------------------------------------------------------------------
    //Delivered remote methods
    //--------------------------------------------------------------------------

    void cloneDeliveredRemoteReads(Set<String> delivered);

    boolean isAlreadyDelivered(EventUID event);

    void addRemoteRead(EventUID event);

    // NEW

    long getNodeId();

    void setLevelsToNodes(Map<Integer, Set<StateTreeProtocol>> levelsToNodes);

    Set<StateTreeProtocol> getLevelsToNodes(Integer level);

    void addDataObjectsToLevel(Set<DataObject> dataObjects, int level);

    Set<DataObject> getDataObjectsFromLevel(int level);

    Set<DataObject> getAllDataObjects();

    Map<Integer, Set<DataObject>> getAllDataObjectsPerLevel();

    void cloneLevelsToNodes(Map<Integer, Set<StateTreeProtocol>> levelsToNodes);

    void cloneLevelToDataObjects(Map<Integer, Set<DataObject>> levelToDataObjects);
}