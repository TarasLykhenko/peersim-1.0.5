package example.saturn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import example.saturn.datastructures.Dictionary;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

public class InitTreeProtocol implements Control {
    // ------------------------------------------------------------------------
    // Parameters
    // ------------------------------------------------------------------------

	private static final String PAR_TREE_PROT = "tree_protocol";
	private static final String PAR_TYPE_PROT = "type_protocol";
    private static final String PAR_DICTIONARY_FILE = "dictionary";
    private static final String PAR_KEYS_NODE = "keys_node";
    private static final String PAR_CLIENTS_CYCLE = "clients_cycle";
    private static final String PAR_MAX_CLIENTS = "max_clients";



    // ------------------------------------------------------------------------
    // Fields
    // ------------------------------------------------------------------------

    private final int tree, type, keysPerNode, clientsCycle, maxClients;
    private final String dictionary_file;


    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    public InitTreeProtocol(String prefix) {
        tree = Configuration.getPid(prefix + "." + PAR_TREE_PROT);
        type = Configuration.getPid(prefix + "." + PAR_TYPE_PROT);
        keysPerNode = Configuration.getInt(prefix + "." + PAR_KEYS_NODE);
        dictionary_file =  Configuration.getString(prefix + "." + PAR_DICTIONARY_FILE);
        clientsCycle = Configuration.getInt(prefix + "." + PAR_CLIENTS_CYCLE);
        maxClients = Configuration.getInt(prefix + "." + PAR_MAX_CLIENTS);
    }

    // ------------------------------------------------------------------------
    // Methods
    // ------------------------------------------------------------------------

    public boolean execute() {
    	Dictionary dictionary = readDictionary(dictionary_file);
    	Map<Integer, TreeSet<Integer>> replicas = dictionary.getReplicas();
    	Map<Integer, List<Integer>> replicasFormated = generateReplicationGroups(replicas);
        Node n = null;
        TreeProtocol treeProtocol = null;
        TypeProtocol typeProtocol = null;
        for (int i = 0; i < Network.size(); i++) {
            n = Network.get(i);
            treeProtocol = (TreeProtocol) n.getProtocol(tree);
            typeProtocol = (TypeProtocol) n.getProtocol(type);
            treeProtocol.initQueue(n);
            treeProtocol.setReplicationGroups(replicasFormated);
            //treeProtocol.setFriendList(dictionary.getGraph().get(i));

            if (typeProtocol.getType()== TypeProtocol.Type.DATACENTER){
            	TreeSet<Integer> set = dictionary.getMasters().get(i);
            	TreeSet<Integer> subset = new TreeSet<>();
            	if (maxClients!=-1){
	            	for (int c = 0; c < maxClients; c++){
	            		subset.add(set.pollFirst());
	            	}
            	}else{
            		subset = set;
            	}
            	//System.out.println(subset);
            	treeProtocol.setClientList(subset, dictionary.getGraph(), keysPerNode);
            	treeProtocol.setClientsCycle(clientsCycle);
            }
        }
        return false;
    }
    
    private Map<Integer, List<Integer>> generateReplicationGroups(Map<Integer, TreeSet<Integer>> replicas){
    	HashMap<Integer, List<Integer>> dictionary = new HashMap<>();
    	
    	for (Integer key : replicas.keySet()){
			TreeSet<Integer> list = replicas.get(key);
			for (Integer node : list) {
				if (dictionary.containsKey(node)) {
					dictionary.get(node).add(key);
				} else {
					ArrayList<Integer> listServers = new ArrayList<>();
					listServers.add(key);
					dictionary.put(node, listServers);
				}
			}
		}
    	return dictionary;
    }
    
    private Dictionary readDictionary(String dictionary_file){
    	Gson gson = new Gson();
    	Dictionary dict = null;
    	try {
    		BufferedReader br = new BufferedReader(new FileReader(dictionary_file));
    		//convert the json string back to object
    		//java.lang.reflect.Type type = new TypeToken<HashMap<String, TreeSet<Integer>>>(){}.getType();
    		dict = gson.fromJson(br, Dictionary.class);
    		
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	return dict;
    }

}
