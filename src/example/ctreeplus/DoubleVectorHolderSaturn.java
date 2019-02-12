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

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Vector;

import peersim.config.Configuration;
import peersim.core.*;

public class DoubleVectorHolderSaturn 
implements DoubleVectorSaturn, Protocol
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
	//protected HashMap<Long, Vector<EventUID>> queue;
	protected HashMap<Long, PriorityQueue<EventUID>> queue;
	protected HashMap<Integer, PriorityQueue<PendingEventUID>> pendingQueue;
	protected Vector<EventUID> processed;
	private static final String PAR_LINK_PROT = "linkable";
	private final int link;
		
	//--------------------------------------------------------------------------
	//Initialization
	//--------------------------------------------------------------------------
	
	/**
	 * Does nothing.
	 */
	public DoubleVectorHolderSaturn(String prefix)
	{
		link = Configuration.getPid(prefix + "." + PAR_LINK_PROT);
		metadata=new VersionVector();
		data=new VersionVector();
		counter=0;
		epoch=0;
		largerEpochSeen=0;
		queue=new HashMap<Long, PriorityQueue<EventUID>>();
		processed=new Vector<EventUID>();
		pendingQueue=new HashMap<Integer, PriorityQueue<PendingEventUID>>();
	}
	
	//--------------------------------------------------------------------------
	
	/**
	 * Clones the value holder.
	 */
	public Object clone()
	{
		DoubleVectorHolderSaturn svh=null;
		try {
			svh=(DoubleVectorHolderSaturn)super.clone();
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
	
	public void cloneQueue(HashMap<Long, PriorityQueue<EventUID>> queueInit){
		queue = new HashMap<Long, PriorityQueue<EventUID>>();
		for (Long key : queueInit.keySet()){
			PriorityQueue<EventUID> clonedQueue = clonePriorityQueue((PriorityQueue<EventUID>) queueInit.get(key));
			queue.put(key, clonedQueue);
		}
	}
	
	public void clonePendingQueue(HashMap<Integer, PriorityQueue<PendingEventUID>> pendingQueueInit){
		pendingQueue = new HashMap<Integer, PriorityQueue<PendingEventUID>>();
		for (int key : pendingQueueInit.keySet()){
			PriorityQueue<PendingEventUID> clonedQueue = clonePriorityPendingQueue((PriorityQueue<PendingEventUID>) pendingQueueInit.get(key));
			pendingQueue.put(key, clonedQueue);
		}
	}
	
	public void cloneMetadata(VersionVector metadataInit){
		metadata = new VersionVector(metadataInit);
	}
	
	public void cloneData(VersionVector dataInit){
		data = new VersionVector(dataInit);
	}
	
	public PriorityQueue<EventUID> clonePriorityQueue(PriorityQueue<EventUID> queue){
		Comparator<EventUID> comparator = new EventUIDComparator();
		PriorityQueue<EventUID> clonedQueue = new PriorityQueue<EventUID>(10, comparator);
		while (!queue.isEmpty()){
			EventUID event = queue.poll();
			clonedQueue.add(new EventUID(event.getNodeid(), event.getTimestamp()));
		}
		return clonedQueue;
	}
	
	public PriorityQueue<PendingEventUID> clonePriorityPendingQueue(PriorityQueue<PendingEventUID> queue){
		Comparator<PendingEventUID> comparator = new PendingEventUIDComparator();
		PriorityQueue<PendingEventUID> clonedQueue = new PriorityQueue<PendingEventUID>(10, comparator);
		while (!queue.isEmpty()){
			PendingEventUID pending = queue.poll();
			EventUID event = pending.getEvent();
			clonedQueue.add(new PendingEventUID(new EventUID(event.getNodeid(), event.getTimestamp()), pending.getSenderId()));
		}
		return clonedQueue;
	}
	
	//--------------------------------------------------------------------------
	// Queue methods
	//--------------------------------------------------------------------------
	
	@Override
	public void initQueue(Node node){
		Linkable linkn = (Linkable)node.getProtocol(link);
		for (int i=0; i<linkn.degree(); i++){
			Node neighbor = linkn.getNeighbor(i);
			Comparator<EventUID> comparator = new EventUIDComparator();
			queue.put(neighbor.getID(), new PriorityQueue<EventUID>(10, comparator));
		}
	}
	
	public void addToQueue(EventUID event, Long from){
		for (Long key : queue.keySet()){
			if (key != from){
				((PriorityQueue<EventUID>)queue.get(key)).add(event);
			}
		}
	}
	
	@Override
	public PriorityQueue<EventUID> getQueue(Long node) {
		return queue.get(node);
	}
	
	public void cleanQueue(Long node) {
		if (queue.containsKey(node)){
			((PriorityQueue<EventUID>)queue.get(node)).clear();
		}
	}
	
	public void processQueue(PriorityQueue<EventUID> queue) {
		while (!queue.isEmpty()){
			this.addMetadata(queue.poll());
		}
	}
	
	public void addQueueToQueue(PriorityQueue<EventUID> queue2, Long from){
		while (!queue2.isEmpty()){
			this.addToQueue(queue2.poll(), from);
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
			Comparator<PendingEventUID> comparator = new PendingEventUIDComparator();
			PriorityQueue<PendingEventUID> v = new PriorityQueue<PendingEventUID>(10, comparator);
			pendingQueue.put(epoch, v);
			v.add(new PendingEventUID(event, senderId));
		}
	}

	public PriorityQueue<PendingEventUID> getPendingQueue(int epoch) {
		return pendingQueue.get(epoch);
	}
	
	public void cleanPendingQueue(int epoch) {
		pendingQueue.remove(epoch);
	}
	
	public void addQueueToPendingQueue(PriorityQueue<EventUID> queue2, int epoch, long senderId){
		while (!queue2.isEmpty()){
			this.addToPendingQueue(queue2.poll(), epoch, senderId);
		}
	}
	
	@Override
	public void processPendingEpoch(int epoch){
		PriorityQueue<PendingEventUID> v = getPendingQueue(epoch);
		if (v!=null){
			//System.out.println("cleaning pendings of epoch "+epoch+" - "+v.toString());
			while (!v.isEmpty()){
				PendingEventUID event = v.poll();
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