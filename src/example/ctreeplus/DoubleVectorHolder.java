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

import peersim.config.Configuration;
import peersim.core.*;

public class DoubleVectorHolder 
implements DoubleVector, Protocol
{

	//--------------------------------------------------------------------------
	//Fields
	//--------------------------------------------------------------------------
		
	/** Value held by this protocol */
	protected VersionVector metadata;
	protected VersionVector data;
	protected int counter;
	protected int epoch;
	protected int largerEpochSeen;
	protected HashMap<Long, Vector<EventUID>> queue;
	protected HashMap<Integer, Vector<PendingEventUID>> pendingQueue;
	protected Vector<EventUID> processed;
	private static final String PAR_LINK_PROT = "linkable";
	private final int link;
		
	//--------------------------------------------------------------------------
	//Initialization
	//--------------------------------------------------------------------------
	
	/**
	 * Does nothing.
	 */
	public DoubleVectorHolder(String prefix)
	{
		link = Configuration.getPid(prefix + "." + PAR_LINK_PROT);
		metadata=new VersionVector();
		data=new VersionVector();
		counter=0;
		epoch=0;
		largerEpochSeen=0;
		queue=new HashMap<Long, Vector<EventUID>>();
		processed=new Vector<EventUID>();
		pendingQueue=new HashMap<Integer, Vector<PendingEventUID>>();
	}
	
	//--------------------------------------------------------------------------
	
	/**
	 * Clones the value holder.
	 */
	public Object clone()
	{
		DoubleVectorHolder svh=null;
		try {
			svh=(DoubleVectorHolder)super.clone();
			svh.cloneQueue(queue);
			svh.clonePendingQueue(pendingQueue);
			svh.cloneProcessed(processed);
			svh.cloneMetadata(metadata);
			svh.cloneData(data);
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//svh = new DoubleVectorHolder("");
		return svh;
	}
	
	//--------------------------------------------------------------------------
	//methods
	//--------------------------------------------------------------------------
	
	
	//--------------------------------------------------------------------------
	// Clone methods
	//--------------------------------------------------------------------------
	public void cloneProcessed(Vector<EventUID> processedInit){
		processed = new Vector<EventUID>();
		for (int i=0; i<processedInit.size(); i++){
			processed.add(processedInit.get(i));
		}
	}
	
	public void cloneQueue(HashMap<Long, Vector<EventUID>> queueInit){
		queue = new HashMap<Long, Vector<EventUID>>();
		for (Long key : queueInit.keySet()){
			queue.put(key, (Vector<EventUID>) queueInit.get(key).clone());
		}
	}
	
	public void clonePendingQueue(HashMap<Integer, Vector<PendingEventUID>> pendingQueueInit){
		pendingQueue = new HashMap<Integer, Vector<PendingEventUID>>();
		for (int key : pendingQueueInit.keySet()){
			pendingQueue.put(key, (Vector<PendingEventUID>) pendingQueueInit.get(key).clone());
		}
	}
	
	public void cloneMetadata(VersionVector metadataInit){
		metadata = new VersionVector(metadataInit);
	}
	
	public void cloneData(VersionVector dataInit){
		data = new VersionVector(dataInit);
	}
	
	//--------------------------------------------------------------------------
	// Queue methods
	//--------------------------------------------------------------------------
	
	@Override
	public void initQueue(Node node){
		
		Linkable linkn = (Linkable)node.getProtocol(link);
		for (int i=0; i<linkn.degree(); i++){
			Node neighbor = linkn.getNeighbor(i);
			queue.put(neighbor.getID(), new Vector<EventUID>());
		}
	}
	
	public void addToQueue(EventUID event, Long from){
		for (Long key : queue.keySet()){
			if (key != from){
				((Vector<EventUID>)queue.get(key)).add(event);
			}
		}
	}
	
	@Override
	public Vector<EventUID> getQueue(Long node) {
		return queue.get(node);
	}
	
	public void cleanQueue(Long node) {
		if (queue.containsKey(node)){
			((Vector<EventUID>)queue.get(node)).clear();
		}
	}
	
	public void processQueue(Vector<EventUID> queue) {
		for (int i=0; i<queue.size(); i++){
			this.addMetadata(queue.get(i));
		}
	}
	
	public void addQueueToQueue(Vector<EventUID> queue2, Long from){
		for (int i=0; i<queue2.size(); i++){
			this.addToQueue(queue2.get(i), from);
		}
	}
	
	//--------------------------------------------------------------------------
	// Pending queue methods
	//--------------------------------------------------------------------------
	
	public void addToPendingQueue(EventUID event, int epoch, long senderId) {
		//System.out.println("Adding event: "+event.toString()+" to node "+senderId+" pending queue");

		if (pendingQueue.containsKey(epoch)){
			pendingQueue.get(epoch).add(new PendingEventUID(event, senderId));
		}else{
			Vector<PendingEventUID> v = new Vector<PendingEventUID>();
			pendingQueue.put(epoch, v);
			v.add(new PendingEventUID(event, senderId));
		}
	}

	public Vector<PendingEventUID> getPendingQueue(int epoch) {
		return pendingQueue.get(epoch);
	}
	
	public void cleanPendingQueue(int epoch) {
		pendingQueue.remove(epoch);
	}
	
	public void addQueueToPendingQueue(Vector<EventUID> queue2, int epoch, long senderId){
		for (int i=0; i<queue2.size(); i++){
			this.addToPendingQueue(queue2.get(i), epoch, senderId);
		}
	}
	
	@Override
	public void processPendingEpoch(int epoch){
		Vector<PendingEventUID> v = getPendingQueue(epoch);
		if (v!=null){
			//System.out.println("cleaning pendings of epoch "+epoch+" - "+v.toString());
			for (int i=0; i<v.size(); i++){
				PendingEventUID event = v.get(i);
				this.addToQueue(event.getEvent(), event.getSenderId());
			}
		}
	}
	
	//--------------------------------------------------------------------------
	// Processed methods
	//--------------------------------------------------------------------------
	
	public void addProcessedEvent(EventUID event){
		this.processed.add(event);
	}
	
	public void cleanProcessed(){
		this.processed.clear();
	}
	
	public Vector<EventUID> getProcessed(){
		return this.processed;
	}
	
	public String processedToString(){
		String result = "";
		for (int i=0; i<processed.size(); i++){
			result=result+" "+processed.get(i).toString();
		}
		return result;
	}
	
	public String processedToStringFileFormat(){
		String result = "";
		for (int i=0; i<processed.size(); i++){
			result=result+" "+processed.get(i).toStringFileFormat();
		}
		return result;
	}
	
	//--------------------------------------------------------------------------
	// Data/metadata methods
	//--------------------------------------------------------------------------
	
	@Override
	public int addMetadata(EventUID event) {
		int result = metadata.addEvent(event.getNodeid(), event.getTimestamp());
		if (data.seenEvent(event.getNodeid(), event.getTimestamp())){
			this.addProcessedEvent(event);
		}
		return result;
	}
	
	@Override
	public VersionVector getMetadataVector(){
		return metadata;
	}

	@Override
	public int addData(EventUID event, Object data) {
		int result = this.data.addEvent(event.getNodeid(), event.getTimestamp());
		if (metadata.seenEvent(event.getNodeid(), event.getTimestamp())){
			this.addProcessedEvent(event);
		}
		return result;
	}
	
	@Override
	public VersionVector getDataVector(){
		return data;
	}
	
	//--------------------------------------------------------------------------
	// Epoch methods
	//--------------------------------------------------------------------------
	
	public int newEpoch(){
		epoch++;
		return epoch;
	}
	
	public int getEpoch(){
		return epoch;
	}
	
	public int largerEpochSeen(){
		return largerEpochSeen;
	}

	public void updateLargerEpochSeen(int newEpoch){
		if (newEpoch>largerEpochSeen){
			largerEpochSeen = newEpoch;
		}
	}
	
	//--------------------------------------------------------------------------
	// Timestamp methods
	//--------------------------------------------------------------------------
	
	public int timestamp(){
		counter++;
		return counter;
	}
}