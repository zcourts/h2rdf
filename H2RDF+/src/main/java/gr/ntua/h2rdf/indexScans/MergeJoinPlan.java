/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.indexScans;

import java.util.HashSet;
import java.util.Set;

public class MergeJoinPlan {
	public double cost;
	public boolean centalized;
	public Set<BGP> scans;
	public Set<ResultBGP> intermediate;
	public long[][] maxPartition;
	public BGP maxBGP;
	
	public String toString(){
		String ret = "";
		if(centalized){
			ret+= "Centralized ";
		}
		else{
			ret+= "MapReduce ";
		}
		ret+=  "Cost: "+cost+"\n";
		ret+=  "Partitions: "+maxPartition.length+"\n";
		ret+=  "Partition BGP: "+maxBGP.bgp+"\n";
		ret+="Scans\n";
		for(BGP b : scans){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		ret+="Intermediate\n";
		for(ResultBGP b : intermediate){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		return ret;
	}

	public MergeJoinPlan() {
		scans = new HashSet<BGP>();
		intermediate = new HashSet<ResultBGP>();
		cost=0;
	}
}
