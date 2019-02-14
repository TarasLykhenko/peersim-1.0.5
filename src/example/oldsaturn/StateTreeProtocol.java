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

package example.oldsaturn;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import example.oldsaturn.datatypes.EventUID;
import example.oldsaturn.datatypes.PendingEventUID;
import example.oldsaturn.datatypes.VersionVector;
import peersim.core.Node;

/**
 * The implementor class has a single parameter. This interface
 * provides access to that parameter.
 */
public interface StateTreeProtocol {

    /**
     * Returns
     * 			0: New and no matching data
     * 			1: New and matching data
     * 			2: Seen
     */
    public void addMetadata(EventUID event);

    /**
     * Returns
     * 			0: New and no matching metadata
     * 			1: New and matching metadata
     * 			2: Seen
     */
    public void addData(EventUID event, Object data);

    public void addQueueToQueue(Vector<EventUID> queue2, Long from);

    public void addToPendingQueue(EventUID event, int epoch, long senderId);

    public void addQueueToPendingQueue(Vector<EventUID> queue2, int epoch, long senderId);

    public Vector<PendingEventUID> getPendingQueue(int epoch);

    public void cleanPendingQueue(int epoch);

    public void processQueue(Vector<EventUID> queue, long id);

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

    public void cloneQueue(Map<Long, Vector<EventUID>> queueInit);

    public void clonePendingQueue(Map<Integer, Vector<PendingEventUID>> pendingQueueInit);

    public void cloneProcessed(Vector<EventUID> processedInit);

    public void cloneMetadata(VersionVector metadataInit);

    public void cloneData(VersionVector dataInit);

    public void addToQueue(EventUID event, Long from);

    Vector<EventUID> getQueue(Long node);

    void cleanQueue(Long node);

    void processPendingEpoch(int epoch);

    VersionVector getMetadataVector();

    VersionVector getDataVector();

    void initQueue(Node node);

    public double getAverageProcessingTime();

    //--------------------------------------------------------------------------
//Replication groups methods
//--------------------------------------------------------------------------
    public void cloneReplicationGroups(Map<Integer, List<Integer>> rgInit);
    public void setReplicationGroups(Map<Integer, List<Integer>> rgInit);
    public List<Integer> getReplicationGroup(Integer key);
    public boolean isInterested(long node, long key);

//--------------------------------------------------------------------------
//Client methods
//--------------------------------------------------------------------------

    public void cloneClients(List<Client> clientsInit);
    public void setClientList(Set<Integer> clientList, Map<Integer, TreeSet<Integer>> friends, int keysPerNode);
    public void setClientsCycle(int clientsCycle);
    public int getClientsCycle();

//--------------------------------------------------------------------------
// Friends methods
//--------------------------------------------------------------------------
/*public void cloneFriends(TreeSet<Integer> friendsInit);
public void setFriendList(TreeSet<Integer> friendsList);*/

    //--------------------------------------------------------------------------
//Statistics
//--------------------------------------------------------------------------
    public void cloneStatistics(int localReads, int remoteReads, int updates, int full, int partial);

    public void incrementUpdates();

    public void incrementRemoteReads();

    public void incrementLocalReads();

    public int getNumberUpdates();

    public int getNumberRemoteReads();

    public int getNumberLocalReads();

    public void addFullMetadata(int update);

    public void addPartialMetadata(int update);

    public double getFullMetadata();

    public double getPartialMetadata();

//--------------------------------------------------------------------------
//Delivered remote methods
//--------------------------------------------------------------------------

    public void cloneDeliveredRemoteReads(Set<String> delivered);

    public boolean isAlreadyDelivered(EventUID event);

    public void addRemoteRead(EventUID event);

}