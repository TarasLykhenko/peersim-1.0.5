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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.Vector;

import peersim.config.Configuration;
import peersim.core.*;

public class DoubleVectorHolderSaturn2 
implements DoubleVectorSaturn2, Protocol
{

	//--------------------------------------------------------------------------
	//Fields
	//--------------------------------------------------------------------------
		
	/** Value held by this protocol */
	protected VersionVector metadata;
	protected Queue<EventUID2> metadataQueue;
	protected VersionVector data;
	protected int counter;
	protected int epoch;
	protected int largerEpochSeen;
	protected double averageProcessing;
	protected HashMap<Long, Vector<EventUID2>> queue;
	protected HashMap<Integer, Vector<PendingEventUID2>> pendingQueue;
	protected Vector<EventUID2> processed;
	private static final String PAR_LINK_PROT = "linkable";
	private final int link;
		
	//--------------------------------------------------------------------------
	//Initialization
	//--------------------------------------------------------------------------
	
	/**
	 * Does nothing.
	 */
	public DoubleVectorHolderSaturn2(String prefix)
	{
		link = Configuration.getPid(prefix + "." + PAR_LINK_PROT);
		metadata=new VersionVector();
		metadataQueue=new LinkedList<EventUID2>();
		data=new VersionVector();
		counter=0;
		epoch=0;
		largerEpochSeen=0;
		queue=new HashMap<Long, Vector<EventUID2>>();
		processed=new Vector<EventUID2>();
		pendingQueue=new HashMap<Integer, Vector<PendingEventUID2>>();
	}
	
	//--------------------------------------------------------------------------
	
	/**
	 * Clones the value holder.
	 */
	public Object clone()
	{
		DoubleVectorHolderSaturn2 svh=null;
		try {
			svh=(DoubleVectorHolderSaturn2)super.clone();
			svh.cloneQueue(queue);
			svh.cloneMetadataQueue(metadataQueue);
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
	public void cloneMetadataQueue(Queue<EventUID2> metadataQueueInit){
		metadataQueue = new LinkedList<EventUID2>();
		Iterator<EventUID2> iterator = metadataQueueInit.iterator();
		while (iterator.hasNext()){
			EventUID2 event = iterator.next();
			metadataQueue.add(new EventUID2(event.getNodeid(), event.getTimestamp(), event.getEpoch()));
		}
	}
	public void cloneProcessed(Vector<EventUID2> processedInit){
		processed = new Vector<EventUID2>();
		for (int i=0; i<processedInit.size(); i++){
			processed.add(processedInit.get(i));
		}
	}
	
	public void cloneQueue(HashMap<Long, Vector<EventUID2>> queueInit){
		queue = new HashMap<Long, Vector<EventUID2>>();
		for (Long key : queueInit.keySet()){
			queue.put(key, (Vector<EventUID2>) queueInit.get(key).clone());
		}
	}
	
	public void clonePendingQueue(HashMap<Integer, Vector<PendingEventUID2>> pendingQueueInit){
		pendingQueue = new HashMap<Integer, Vector<PendingEventUID2>>();
		for (int key : pendingQueueInit.keySet()){
			pendingQueue.put(key, (Vector<PendingEventUID2>) pendingQueueInit.get(key).clone());
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
			queue.put(neighbor.getID(), new Vector<EventUID2>());
		}
	}
	
	public void addToQueue(EventUID2 event, Long from){
		for (Long key : queue.keySet()){
			if (key != from){
				((Vector<EventUID2>)queue.get(key)).add(event);
			}
		}
	}
	
	@Override
	public Vector<EventUID2> getQueue(Long node) {
		return queue.get(node);
	}
	
	public void cleanQueue(Long node) {
		if (queue.containsKey(node)){
			((Vector<EventUID2>)queue.get(node)).clear();
		}
	}
	
	public void processQueue(Vector<EventUID2> queue, long id) {
		for (int i=0; i<queue.size(); i++){
			int switchId = (int) id;
			EventUID2 event = queue.get(i);
			switch (switchId) {
			case 0:	
				if ((event.getNodeid() == 1)||(event.getNodeid() == 2)){
					addMetadata(queue.get(i));
				}
				break;
			case 1:
				if (event.getNodeid() == 0){
					addMetadata(queue.get(i));
				}
				break;
			case 2:
				if ((event.getNodeid() == 0)||(event.getNodeid() == 3)){
					addMetadata(queue.get(i));
				}
				break;
			case 3:
				if (event.getNodeid() == 2){
					addMetadata(queue.get(i));
				}
				break;
			}
		}
	}
	
	public void addQueueToQueue(Vector<EventUID2> queue2, Long from){
		for (int i=0; i<queue2.size(); i++){
			this.addToQueue(queue2.get(i), from);
		}
	}
	
	//--------------------------------------------------------------------------
	// Pending queue methods
	//--------------------------------------------------------------------------
	
	public void addToPendingQueue(EventUID2 event, int epoch, long senderId) {
		//System.out.println("Adding event: "+event.toString()+" to node "+senderId+" pending queue");
		if (pendingQueue.containsKey(epoch)){
			pendingQueue.get(epoch).add(new PendingEventUID2(event, senderId));
		}else{
			Vector<PendingEventUID2> v = new Vector<PendingEventUID2>();
			pendingQueue.put(epoch, v);
			v.add(new PendingEventUID2(event, senderId));
		}
	}

	public Vector<PendingEventUID2> getPendingQueue(int epoch) {
		return pendingQueue.get(epoch);
	}
	
	public void cleanPendingQueue(int currentEpoch) {
		Set<Integer> epochs = pendingQueue.keySet();
		Integer[] list = new Integer[epochs.size()];
		int c = 0;
		for (int element : epochs){
			list[c]=element;
			c++;
		}
		for (int epoch : list){
			if (epoch<currentEpoch){
				pendingQueue.remove(epoch);
			}
		}
	}
	
	public void addQueueToPendingQueue(Vector<EventUID2> queue2, int epoch, long senderId){
		for (int i=0; i<queue2.size(); i++){
			this.addToPendingQueue(queue2.get(i), epoch, senderId);
		}
	}
	
	@Override
	public void processPendingEpoch(int currentEpoch){
		for (int epoch : pendingQueue.keySet()){
			if (epoch<currentEpoch){
				Vector<PendingEventUID2> v = getPendingQueue(epoch);
				if (v!=null){
					//System.out.println("cleaning pendings of epoch "+epoch+" - "+v.toString());
					for (int i=0; i<v.size(); i++){
						PendingEventUID2 event = v.get(i);
						this.addToQueue(event.getEvent(), event.getSenderId());
					}
				}
			}
		}
	}
	
	//--------------------------------------------------------------------------
	// Processed methods
	//--------------------------------------------------------------------------
	
	public void addProcessedEvent(EventUID2 event){
		averageProcessing = (averageProcessing + (getEpoch() - event.getEpoch()))/2;
		this.processed.add(event);
	}
	
	public void cleanProcessed(){
		this.processed.clear();
	}
	
	public Vector<EventUID2> getProcessed(){
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
	
	public double getAverageProcessingTime(){
		return averageProcessing;
	}
	
	//--------------------------------------------------------------------------
	// Data/metadata methods
	//--------------------------------------------------------------------------
	
	@Override
	public void addMetadata(EventUID2 event) {
		if (metadataQueue.isEmpty()){
			if (data.seenEvent(event.getNodeid(), event.getTimestamp())){
				addProcessedEvent(event);
			}else{
				metadataQueue.add(event);
			}
		}else{
			metadataQueue.add(event);
		}
	}
	
	@Override
	public VersionVector getMetadataVector(){
		return metadata;
	}

	@Override
	public void addData(EventUID2 event, Object datum) {
		data.addEvent(event.getNodeid(), event.getTimestamp());
		boolean matches = true;
		while (!metadataQueue.isEmpty()&&(matches)){
			EventUID2 head = metadataQueue.peek();
			if (data.seenEvent(head.getNodeid(), head.getTimestamp())){
				metadataQueue.poll();
				addProcessedEvent(head);
			}else{
				matches = false;
			}
		}
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