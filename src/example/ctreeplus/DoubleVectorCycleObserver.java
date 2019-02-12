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

import peersim.config.*;
import peersim.core.*;
import peersim.util.*;

/**
* Print statistics over a vector. The vector is defined by a protocol,
* specified by {@value #PAR_PROT}, that has to  implement
* {@link SingleValue}.
* Statistics printed are: min, max, number of samples, average, variance,
* number of minimal instances, number of maximal instances (using
* {@link IncrementalStats#toString}).
* @see IncrementalStats
*/
public class DoubleVectorCycleObserver implements Control {


//--------------------------------------------------------------------------
//Parameters
//--------------------------------------------------------------------------

/** 
 *  The parameter used to determine the accuracy
 *  (standard deviation) before stopping the simulation. If not 
 *  defined, a negative value is used which makes sure the observer 
 *  does not stop the simulation.
 * @see #execute
 *  @config
 */
private static final String PAR_ACCURACY = "accuracy";

/**
 * The protocol to operate on.
 * @config
 */
private static final String PAR_PROT = "protocol";


//--------------------------------------------------------------------------
// Fields
//--------------------------------------------------------------------------


/** Protocol identifier */
private final int pid;
private int iteration;


//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------

/**
 * Standard constructor that reads the configuration parameters.
 * Invoked by the simulation engine.
 * @param name the configuration prefix for this class
 */
public DoubleVectorCycleObserver(String name)
{
	pid = Configuration.getPid(name + "." + PAR_PROT);
	iteration = 1;
}


//--------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------

/**
* Print statistics over a vector. The vector is defined by a protocol,
* specified by {@value #PAR_PROT}, that has to  implement
* {@link SingleValue}.
* Statistics printed are: min, max, number of samples, average, variance,
* number of minimal instances, number of maximal instances (using 
* {@link IncrementalStats#toString}).
* @return true if the standard deviation is below the value of
 * {@value #PAR_ACCURACY}, and the time of the simulation is larger then zero
 * (i.e. it has started).
 */
public boolean execute(){

		/* Compute max, min, average */
		System.out.println("Observer init ======================");
		for (int i = 0; i < Network.size(); i++)
		{
			DoubleVector v = (DoubleVector)Network.get(i).getProtocol(pid);
			System.out.println("[Node "+i+"]=========================");
			System.out.println("Processed "+v.processedToString());
			System.out.println("Metadata "+v.getMetadataVector().toString());
			System.out.println("Data "+v.getDataVector().toString());
		}
		System.out.println("Observer end =======================");
		/* Terminate if accuracy target is reached */
		iteration++;
		return false;
		/*return (stats.getStD()<=accuracy && CommonState.getTime()>0); */
}

//--------------------------------------------------------------------------

}
