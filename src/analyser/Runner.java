package analyser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import peersim.core.Node;
import example.ctreeplus.InetType;

public class Runner {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		switch (args.length) {
		case 0: 
			printHelp();
			break;
		case 1: 
			printHelp();
			break;
		case 2:
			String statsFile = args[0];
			int upto = Integer.parseInt(args[1]);
			System.out.println("Analysing up to "+upto+" events");
			BufferedReader br = null;
	        try {
	        	br = new BufferedReader(new FileReader(statsFile));
	            String line = br.readLine();
	            while (line != null) {
	            	HashMap<Long, Integer> counters = new HashMap<Long, Integer>();
	                String[] parts = line.split(" ");
	                Long node = Long.parseLong(parts[0]);    
	                System.out.println("Node "+node+" (total received: "+(parts.length - 1)+")");
	                if ((parts.length-1)>=1){
		                for (int i=1; i<upto; i++) {
		                	if (parts.length>i) {
			                	String[] event = parts[i].split(",");
			                	Long peer = Long.valueOf(event[0]);
			                	if (counters.containsKey(peer)){
			                		counters.put(peer, counters.get(peer)+1);
			                	}else{
			                		counters.put(peer, 1);
		                	}
		                	}
		                }
	                }
	                for (Long key : counters.keySet()){
	                	System.out.println(counters.get(key)+" from node "+key);
	                }
	                line = br.readLine();
	            }
	            br.close();
	        } catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
				break;
		}
	}
	
	private static void printHelp(){
		System.out.println("C+Tree Stats Analyser");
		System.out.println("=====================");
		System.out.println("Args:");
		System.out.println();
		System.out.println("# String: stats file");
		System.out.println("# Integer: limit of received updates to analyse");
	}
}
