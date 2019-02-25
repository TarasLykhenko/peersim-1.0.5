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

import example.common.BasicStateTreeProtocol;
import example.common.datatypes.DataObject;
import example.occult.datatypes.OccultMasterWrite;
import example.occult.datatypes.OccultReadResult;

import java.util.Map;
import java.util.Set;

/**
 * The implementor class has a single parameter. This interface
 * provides access to that parameter.
 */
public interface StateTreeProtocol extends BasicStateTreeProtocol {

    int timestamp();

    void setNodeId(Long nodeId);

    long getNodeId();

    //--------------------------------------------------------------------------
    //Replication groups methods
    //--------------------------------------------------------------------------

    boolean isInterested(int key);

    //--------------------------------------------------------------------------
    //Client methods
    //--------------------------------------------------------------------------

    void addClients(Set<OccultClientInterface> clientList);

    Set<OccultClientInterface> getClients();

    //--------------------------------------------------------------------------
    //Delivered remote methods
    //--------------------------------------------------------------------------


    // NEW

    void setLevelsToNodes(Map<Integer, Set<StateTreeProtocol>> levelsToNodes);

    Set<StateTreeProtocol> getLevelsToNodes(Integer level);

    void addDataObjectsToLevel(Set<DataObject> dataObjects, int level);

    Set<DataObject> getDataObjectsFromLevel(int level);

    Set<DataObject> getAllDataObjects();

    Map<Integer, Set<DataObject>> getAllDataObjectsPerLevel();

    // OCCULT Methods

    OccultReadResult occultRead(Integer key);

    /**
     * Value is skipped because we don't care for values.
     */
    OccultMasterWrite occultWriteMaster(int key, Map<Integer, Integer> deps, int catchAll);

    void occultWriteSlave(int key,
                          Map<Integer, Integer> deps,
                          int catchAll,
                          int shardStamp);

    Map<Integer, Integer> getShardStamps();


}