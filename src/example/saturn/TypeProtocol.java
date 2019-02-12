
package example.saturn;

import peersim.core.Protocol;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores the type of each node in the system
 * @author bravogestoso
 *
 * Type 0: Datacenter
 * Type 1: Broker
 *
 */
public class TypeProtocol implements Protocol {

    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    //private int type;
    private Type type;
    private Map<Long, Integer> latencies;

    enum Type {
        DATACENTER,
        BROKER
    }

    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------
    /**
     * Standard constructor that reads the configuration parameters. Invoked by
     * the simulation engine. By default, all the coordinates components are set
     * to -1 value. The {@link InitTypeProtocol} class provides a coordinates
     * initialization.
     * 
     * @param prefix
     *            the configuration prefix for this class.
     */
    public TypeProtocol(String prefix) {
        /* Un-initialized coordinates defaults to -1. */
        type = null;
        latencies = new HashMap<>();
    }

    public Object clone() {
        TypeProtocol inp = null;
        try {
            inp = (TypeProtocol) super.clone();
            inp.cloneLatencies(latencies);
        } catch (CloneNotSupportedException e) {
        } // never happens
        return inp;
    }
    
    public void cloneLatencies(Map<Long, Integer> latenciesInit){
        latencies = new HashMap<>();
    	for (Long key : latenciesInit.keySet()){
    		latencies.put(key, latenciesInit.get(key));
    	}
    }
    

    public void setLatency(long to, int latency){
    	latencies.put(to, latency);
    }
    
    public int getLatency(long to){
        return latencies.getOrDefault(to, -1);
    }
    
    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }
}
