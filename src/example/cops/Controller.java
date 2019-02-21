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

import example.common.AbstractController;
import example.common.BasicClientInterface;
import peersim.core.Network;

import java.io.IOException;
import java.util.Set;

public class Controller extends AbstractController {

//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------

    public Controller(String name) throws IOException {
        super(name);
    }


    public void doAdditionalExecution(Set<BasicClientInterface> clients) {
        int totalPendingClients = 0;

        for (int i = 0; i < Network.size(); i++) {
            StateTreeProtocolInstance v = (StateTreeProtocolInstance) Network.get(i).getProtocol(pid);

            /*
            protocol.writer.println("----- END RESULTS -----");
            for (StateTreeProtocolInstance.RemoteUpdateQueueEntry entry : protocol.updateToContextDeps.keySet()) {
                protocol.writer.println("k:" + entry.key + "|v:" + entry.version +
                        " deps: " +protocol.updateToContextDeps.get(entry));
            }
            for (Integer key : protocol.keyToDOVersion.keySet()) {
                protocol.writer.println("Have key:" + key + "|v:" + protocol.keyToDOVersion.get(key));
            }
            protocol.writer.close();
            */


            totalPendingClients += v.clientToDepsQueue.size();
        }

        printImportant(totalPendingClients + "/" + (clients.size() + totalPendingClients) + " pending clients");
    }

    public void doEndExecution(Set<BasicClientInterface> clients) {
        for (BasicClientInterface basicClient : clients) {
            Client client = (Client) basicClient;
            System.out.println("Client " + client.getId() + " locality: " + client.locality + " | getLavgReadLatency: " +
                    ((float) client.readsTotalLatency / client.numberReads)
                    + " | reads: " + client.numberReads +  " | avgUpdateLatency: " +
                    ((float) client.updatesTotalLatency / client.numberUpdates)
                    + " | updates: " + client.numberUpdates) ;
        }
    }
}