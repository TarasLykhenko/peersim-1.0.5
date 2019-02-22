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

import example.common.BasicStateTreeProtocol;
import example.common.datatypes.DataObject;
import example.genericsaturn.datatypes.EventUID;
import example.genericsaturn.datatypes.PendingEventUID;
import example.genericsaturn.datatypes.VersionVector;
import peersim.core.Node;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The implementor class has a single parameter. This interface
 * provides access to that parameter.
 */
public interface StateTreeProtocol extends BasicStateTreeProtocol {

    boolean isInterested(int key);
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

    void addQueueToQueue(List<EventUID> queue2, Long from);

    void addToPendingQueue(EventUID event, int epoch, long senderId);

    void addQueueToPendingQueue(List<EventUID> queue2, int epoch, long senderId);

    List<PendingEventUID> getPendingQueue(int epoch);

    void cleanPendingQueue(int epoch);

    void processQueue(List<EventUID> queue, long id);

    int timestamp();

    int newEpoch();

    int getEpoch();

    int largerEpochSeen();

    void updateLargerEpochSeen(int newEpoch);

    void addProcessedEvent(EventUID event);

    void cleanProcessed();

    List<EventUID> getProcessed();

    String processedToString();

    String processedToStringFileFormat();

    void addToQueue(EventUID event, Long from);

    List<EventUID> getQueue(Long node);

    void cleanQueue(Long node);

    void processPendingEpoch(int epoch);

    VersionVector getMetadataVector();

    VersionVector getDataVector();

    void initQueue(Node node);

    double getAverageProcessingTime();

    //--------------------------------------------------------------------------
    //Client methods
    //--------------------------------------------------------------------------

    void addClients(Set<Client> clientList);

    void setClientsCycle(int clientsCycle);

    int getClientsCycle();

    //--------------------------------------------------------------------------
    //Statistics
    //--------------------------------------------------------------------------

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
}