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

import peersim.core.Protocol;

/**
 * <p>
 * This class does nothing. It is simply a container inside each node to collect
 * peer coordinates.
 * </p>
 * <p>
 * The actual "who knows whom" relation (the topology) container is decoupled
 * from the HOT package. It is maintained by any {@link peersim.core.Linkable} 
 * implementing
 * protocol declared in the config file.
 * </p>
 * 
 * @author Gian Paolo Jesi
 */
public class InetType implements Protocol {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private int type;
    private HashMap<Long, Integer> latencies;
    private HashMap<Long, Integer> frequencies;

    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------
    /**
     * Standard constructor that reads the configuration parameters. Invoked by
     * the simulation engine. By default, all the coordinates components are set
     * to -1 value. The {@link InetInitializer} class provides a coordinates
     * initialization.
     * 
     * @param prefix
     *            the configuration prefix for this class.
     */
    public InetType(String prefix) {
        /* Un-initialized coordinates defaults to -1. */
        type = -1;
        latencies = new HashMap<Long, Integer>();
        frequencies = new HashMap<Long, Integer>();
    }

    public Object clone() {
        InetType inp = null;
        try {
            inp = (InetType) super.clone();
            inp.cloneLatencies(latencies);
            inp.cloneFrequencies(frequencies);
        } catch (CloneNotSupportedException e) {
        } // never happens
        return inp;
    }
    
    public void cloneLatencies(HashMap<Long, Integer> latenciesInit){
        latencies = new HashMap<Long, Integer>();
    	for (Long key : latenciesInit.keySet()){
    		latencies.put(key, latenciesInit.get(key));
    	}
    }
    
    public void cloneFrequencies(HashMap<Long, Integer> frequenciesInit){
        frequencies = new HashMap<Long, Integer>();
    	for (Long key : frequenciesInit.keySet()){
    		frequencies.put(key, frequenciesInit.get(key));
    	}
    }

    public void setLatency(long to, int latency){
    	latencies.put(to, latency);
    }
    
    public int getLatency(long to){
    	if (latencies.containsKey(to)){
    		return latencies.get(to);
    	}else{
    		return -1;
    	}
    }
    
    public void setFrequency(long to, int freq){
    	frequencies.put(to, freq);
    }
    
    public int getFrequency(long to){
    	if (frequencies.containsKey(to)){
    		return frequencies.get(to);
    	}else{
    		return -1;
    	}
    }
    
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
