/*******************************************************************************
 * Copyright 2014 Nikolaos Papailiou
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

public class CachedResults implements DPJoinPlan{
	public List<CachedResult> results;
	private Double cost;
	private CachedResult minResult;
	
	public CachedResults() {
		results= new ArrayList<CachedResult>();
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		return cost.compareTo(o.getCost());
	}

	@Override
	public String print() {
		String ret="{ ";
		for(CachedResult res : results){
			ret+=res.print()+", ";
		}
		ret=ret.substring(0, ret.length()-2);
		ret+=" }";
		return ret;
	}

	@Override
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws Exception {
		minResult.execute(visitor, cachingExecutor);
	}

	@Override
	public Double getCost() {
		/*if(selectiveBindings!=null){
			return results.get(0).stats[0]*results.get(0).stats[1];
		}
		else*/
			return results.get(0).getCost();
	}

	@Override
	public double[] getStatistics(Integer joinVar) throws IOException {
		return results.get(0).getStatistics(joinVar);
	}

	@Override
	public List<ResultBGP> getResults() throws IOException {
		return minResult.getResults();
	}
	public void executeSelection(
			OptimizeOpVisitorDPCaching optimizeOpVisitorDPCaching,
			CachingExecutor cachingExecutor) {
		
	}
	@Override
	public void computeCost() throws IOException {
		cost = Double.MAX_VALUE;
		minResult = null;
		for(CachedResult cr : results){
			cr.computeCost();
			Double c = cr.getCost();
			if(c<cost){
				cost=c;
				minResult=cr;
			}
		}
		//System.out.println("Cost: "+cost);
	}
	@Override
	public List<Integer> getOrdering() {
		return results.get(0).getOrdering();
	}
	
	public CachedResult getResultWithOrdering(List<Integer> ordering) {
		//System.out.println("Checking ordering:");
		CachedResult ret = null;
		String o = "";
		for(Integer i : ordering){
			o+=i+"_";
		}
		System.out.println("Request: "+o);
		for(CachedResult cr : results){
			if(cr.ordered){
				String co = cr.getCanonicalOrderingString();
				//String co = cr.getOrdering().toString();
				System.out.println("CachedOrdering: "+co);
				if(co.equals(o)){
					ret=cr;
					//System.out.println("found");
				}
			}
		}
		return ret;
	}
	
}
