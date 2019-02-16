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

package example.cops;

import example.cops.datatypes.DataObject;
import example.cops.datatypes.EventUID;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The implementor class has a single parameter. This interface
 * provides access to that parameter.
 */
public interface StateTreeProtocol {

    int timestamp();

    void setNodeId(Long nodeId);

    void addProcessedEvent(EventUID event);

    void cleanProcessed();

    List<EventUID> getProcessed();

    String processedToString();

    String processedToStringFileFormat();

    double getAverageProcessingTime();
    //--------------------------------------------------------------------------
    //Replication groups methods
    //--------------------------------------------------------------------------

    boolean isInterested(int key);

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

    // COPS Methods

    int copsGet(Integer key);

    int copsPut(Integer key, long time);

    void copsPutRemote(Integer key, Map<Integer, Integer> context, Integer version);
}