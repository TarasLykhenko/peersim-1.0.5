package example.genericsaturn;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

/**
 * Assigns types to nodes
 * @author bravogestoso
 *
 */
public class InitTypeProtocol implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------
    /**
     * The protocol to operate on.
     * 
     * @config
     */
	private static final String PAR_TYPE_PROT = "type_protocol";
    private static final String PAR_NDATANODES = "ndatanodes";


    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    /** Protocol identifier, obtained from config property { #PAR_PROT}. */
    private final int pid;
    private final int n_datanodes;


    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    /**
     * Standard constructor that reads the configuration parameters. Invoked by
     * the simulation engine.
     * 
     * @param prefix
     *            the configuration prefix for this class.
     */
    public InitTypeProtocol(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
        n_datanodes =  Configuration.getInt(PAR_NDATANODES);
    }

    // ------------------------------------------------------------------------
    // Methods
    // ------------------------------------------------------------------------


    public boolean execute() {
        for (int i = 0; i < n_datanodes; i++) {
            Node node = Network.get(i);
            TypeProtocol prot = (TypeProtocol) node.getProtocol(pid);
            prot.setType(TypeProtocol.Type.DATACENTER);
        }
        for (int i = n_datanodes; i < Network.size(); i++) {
            Node node = Network.get(i);
            TypeProtocol prot = (TypeProtocol) node.getProtocol(pid);
            prot.setType(TypeProtocol.Type.BROKER);
        }
        return false;
    }
}
