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

import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.JoinExecutor;
import gr.ntua.h2rdf.indexScans.MROrderingExecutor;
import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class OrderBy implements DPJoinPlan{
	public DPJoinPlan subplan;
	private List<Integer> orderVarsInt;
	private List<Var> orderVars;
	private BitSet edges;
	private OptimizeOpVisitorDPCaching visitor;
	private CachingExecutor cachingExecutor;
	private Double cost;
	private double[] stats;
	public List<ResultBGP> results;
	public boolean centralized;
	public long[][] maxPartition;
	private final int MRoffset=25;
	private boolean canMerge;
	public boolean toBeIndexed;
	
	public OrderBy(List<Var> orderVars, OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) {
		this.visitor =visitor;
		this.orderVars=orderVars;
		this.orderVarsInt = visitor.orderVarsInt;
		this.cachingExecutor = cachingExecutor;
		toBeIndexed = false;
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		return cost.compareTo(o.getCost());
	}

	@Override
	public String print() {
		String ret="";
		if(canMerge){
			ret =subplan.print();
		}
		else{
			ret = "{Order by vars:"+orderVarsInt+" centralized: "+
	    		centralized+" cost:"+cost+": \nScans: \n";
			ret+=subplan.print()+"\n}";
		}
		return ret;
	}

	@Override
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws Exception {
		if(canMerge){
			//push order info to subplan
		}
		subplan.execute(visitor, cachingExecutor);
		if(!canMerge){
			results = JoinExecutor.executeOrdering(subplan.getResults().get(0), maxPartition, orderVarsInt, centralized, Bytes.toString(visitor.table.getTableName()), visitor);

			CachedResult res = new CachedResult(results, results.get(0).stats,visitor);
			res.setOrdering(orderVarsInt, cachingExecutor);

		}
		
	}

	@Override
	public Double getCost() {
		return cost;
	}

	@Override
	public double[] getStatistics(Integer joinVar) throws IOException {
		return stats;
	}

	@Override
	public List<ResultBGP> getResults() throws IOException {
		return results;
	}

	@Override
	public void computeCost() throws IOException {
		Var firstVar = orderVars.get(0);
    	Integer fv =visitor.varRevIds.get(firstVar);
		stats = subplan.getStatistics(fv);
		
		List<Integer> subOrdering = subplan.getOrdering();
		int k=0;
		canMerge=true;
		for(Integer i : orderVarsInt){
			if(k>=subOrdering.size())
				break;
			if(i!=subOrdering.get(k)){
				canMerge=false;
				break;
			}
			k++;
		}
		if(subOrdering.size()==0){
			canMerge=false;
		}
		canMerge=false;
		if(canMerge){
	    	//System.out.println(subplan.print(visitor, cachingExecutor));
	    	cost=subplan.getCost();
	    	//System.out.println("Cost: "+cost);
	    	//System.out.println("Stats: "+stats[0]+" "+stats[1]);
	    	return;
		}
		
    	int plength = 0;
    	//find max partition
		for(BGP b : visitor.bgpIds.values()){
			if(b.joinVars.contains(firstVar)){
				long[][] p = b.getPartitions(firstVar);
				if(p.length>plength){
					maxPartition=p;
					plength=p.length;
				}
			}
		}
    	cost=subplan.getCost();
    	double costCent = 0, costMR=MRoffset;
    	costCent+= stats[0]*stats[1]*2/100000;
    	costMR+= stats[0]*stats[1]*2/100000/visitor.workers;
    	//System.out.println(subplan.print(visitor, cachingExecutor));
    	//System.out.println("Cost: "+cost);
    	//System.out.println("Stats: "+stats[0]+" "+stats[1]);
    	//System.out.println(costCent+" "+costMR);
    	if(costCent< costMR){
    		cost+= costCent;
    		centralized=true;
    	}
    	else{
    		cost+=costMR;
    		centralized=false;
    	}
		
	}

	public void setEdgeGraph(BitSet edges) {
		this.edges=edges;
	}

	@Override
	public List<Integer> getOrdering() {
		return orderVarsInt;
	}

}
