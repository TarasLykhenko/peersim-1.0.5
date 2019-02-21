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

import example.common.BasicStateTreeProtocol;
import example.common.datatypes.DataObject;
import example.cops.datatypes.EventUID;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The implementor class has a single parameter. This interface
 * provides access to that parameter.
 */
public interface StateTreeProtocol extends BasicStateTreeProtocol {

    int timestamp();

    //--------------------------------------------------------------------------
    //Replication groups methods
    //--------------------------------------------------------------------------

    boolean isInterested(int key);

    //--------------------------------------------------------------------------
    //Client methods
    //--------------------------------------------------------------------------

    void addClients(Set<Client> clientList);


    //--------------------------------------------------------------------------
    //Delivered remote methods
    //--------------------------------------------------------------------------

    boolean isAlreadyDelivered(EventUID event);

    void addRemoteRead(EventUID event);


    void setLevelsToNodes(Map<Integer, Set<StateTreeProtocol>> levelsToNodes);

    Set<StateTreeProtocol> getLevelsToNodes(Integer level);

    void addDataObjectsToLevel(Set<DataObject> dataObjects, int level);

    Set<DataObject> getDataObjectsFromLevel(int level);

    Set<DataObject> getAllDataObjects();

    Map<Integer, Set<DataObject>> getAllDataObjectsPerLevel();

    // COPS Methods

    int copsGet(Integer key);

    int copsPut(Integer key);

    void copsPutRemote(Integer key, Map<Integer, Integer> context, Integer version);
}