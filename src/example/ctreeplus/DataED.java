/*
 * Copyright (c) 2003 The BISON Project
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

import java.util.Vector;

import peersim.config.*;
import peersim.core.*;
import peersim.transport.Transport;
import peersim.cdsim.CDProtocol;
import peersim.edsim.EDProtocol;

/**
* Event driven version of epidemic averaging.
*/
public class DataED extends DoubleVectorHolder
implements CDProtocol, EDProtocol {
	
	  private final int typePid;
	  private static final String PAR_TYPE_PROT = "type_protocol";

//--------------------------------------------------------------------------
// Initialization
//--------------------------------------------------------------------------

/**
 * @param prefix string prefix for config properties
 */
public DataED(String prefix) {
	super(prefix); 
	typePid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
	}

//--------------------------------------------------------------------------
// methods
//--------------------------------------------------------------------------

/**
 * This is the standard method the define periodic activity.
 * The frequency of execution of this method is defined by a
 * {@link peersim.edsim.CDScheduler} component in the configuration.
 */
public void nextCycle( Node node, int pid )
{
	//System.out.println("About to clean pendings, node "+node.getID()+", epoch"+getEpoch());
	processPendingEpoch(getEpoch()); 
	cleanPendingQueue(getEpoch());
	newEpoch();
	Linkable linkable = 
			(Linkable) node.getProtocol( FastConfig.getLinkable(pid) );
	//System.out.println("Node "+node.getIndex()+"(ID: "+node.getID()+" and degree: "+linkable.degree()+"): I am sending and event");
	InetType sendertype = (InetType)node.getProtocol(typePid);
	if (sendertype.getType()==0){
		EventUID event = new EventUID(node.getID(), timestamp());
		for (int i=0; i<linkable.degree(); i++){
			Node peern = linkable.getNeighbor(i);
			// XXX quick and dirty handling of failures
			// (message would be lost anyway, we save time)
			if(!peern.isUp()) return;
			InetType ntype = (InetType) peern.getProtocol(typePid);
			if ((getEpoch() % ntype.getFrequency(node.getID())) == 0 ){
				if (ntype.getType()==0){
					((Transport)node.getProtocol(FastConfig.getTransport(pid))).
					send(node, peern, new DataMessage(event, "data", getEpoch()), pid);
				}else{
					((Transport)node.getProtocol(FastConfig.getTransport(pid))).
					send(node, peern, new MetadataMessage(event, getEpoch(), node.getID()), pid);
				}
			}
		}
	}else{
		for (int i=0; i<linkable.degree(); i++){
			Node peern = linkable.getNeighbor(i);
			InetType ntype = (InetType) peern.getProtocol(typePid);
			if ((getEpoch() % ntype.getFrequency(node.getID())) == 0 ){
				if(!peern.isUp()) return;
				Vector<EventUID> queueToSend = queue.get(peern.getID());
				//System.out.println("Sending queue "+queueToSend+" to node "+peern.getID()+" from node "+node.getID());
				if (queueToSend != null){
					((Transport)node.getProtocol(FastConfig.getTransport(pid))).
					send(node, peern, new QueueMessage((Vector<EventUID>) queueToSend.clone(), ntype.getType(), getEpoch(), node.getID()), pid);
					this.cleanQueue(peern.getID());
				}
			}
		}
	}
}

//--------------------------------------------------------------------------

	/**
	* This is the standard method to define to process incoming messages.
	*/
	public void processEvent( Node node, int pid, Object event) {
		if (event instanceof DataMessage) {
			DataMessage msg = (DataMessage) event;
			this.addData(msg.event, msg.data);
		}else if(event instanceof MetadataMessage) {
			MetadataMessage msg = (MetadataMessage) event;
			//System.out.println("Node "+node.getID()+"in epoch "+getEpoch()+" has received a metadata "+msg.event.toString()+" from Node "+msg.senderId+" in epoch "+msg.epoch);
			if (msg.epoch==this.getEpoch()){
				//System.out.println("About to insert to the queue");
				this.addToQueue(msg.event, msg.senderId);
			}else{
				this.addToPendingQueue(msg.event, msg.epoch, msg.senderId);
			}
			
		}else if(event instanceof QueueMessage) {
			QueueMessage msg = (QueueMessage) event;
			//System.out.println("Node "+node.getID()+" has received a queue "+msg.queue.toString()+" from Node "+msg.senderId);
			if (msg.type==0){
				this.processQueue(msg.queue);
			}else{
				if (msg.epoch==this.getEpoch()){
					this.addQueueToQueue(msg.queue, msg.senderId);
				}else{
					this.addQueueToPendingQueue(msg.queue, msg.epoch, msg.senderId);
				}
			}
		}
	}

//--------------------------------------------------------------------------
//--------------------------------------------------------------------------

	class DataMessage {
	
		final EventUID event;
		final Object data;
		final int epoch;
		public DataMessage( EventUID event, Object data, int epoch)
		{
			this.event = event;
			this.data = data;
			this.epoch = epoch;
		}
	}
	
	class MetadataMessage {
		
		final EventUID event;
		final int epoch;
		final long senderId;
		
		public MetadataMessage(EventUID event, int epoch, long senderId)
		{
			this.event=event;
			this.epoch = epoch;
			this.senderId = senderId;
		}
	}
	
	class QueueMessage {

		final Vector<EventUID> queue;
		final int type;
		final int epoch;
		final long senderId;

		public QueueMessage(Vector<EventUID> queue, int type, int epoch, long senderId)
		{
			this.queue = queue;
			this.type = type;
			this.epoch = epoch;
			this.senderId = senderId;
		}
	}
	
}

