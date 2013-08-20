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

import java.util.Set;

public class JoinPlan {
	public double cost;
	public boolean centalized;
	public Set<ResultBGP> Map, Reduce;
	
	public String toString(){
		String ret = "";
		if(centalized){
			ret+= "Centralized ";
		}
		else{
			ret+= "MapReduce ";
		}
		ret+=  "Cost: "+cost+"\n";
		ret+="Map\n";
		for(ResultBGP b : Map){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		ret+="Reduce\n";
		for(ResultBGP b : Reduce){
			if(b.bgp==null)
				ret+=b.print()+"\n";
			else
				ret+=b.bgp.toString()+"\n";
		}
		return ret;
	}
}
