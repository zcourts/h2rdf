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
import gr.ntua.h2rdf.indexScans.MergeJoinPlan;
import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.sparql.core.Var;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;

public class MergeJoin implements DPJoinPlan{

	private Integer var;
	private List<IndexScan> scans;
	private List<CachedResult> resultScans;
	private BitSet edges;
	private Double cost;
	private OptimizeOpVisitorDPCaching visitor;
	private CachingExecutor cachingExecutor;
	public long[][] maxPartition;
	public double n;
	private final int MRoffset=25;
	public DPJoinPlan maxScan;
	private double[] stats;
	public List<ResultBGP> results;
	public boolean centralized;
	
	
	public MergeJoin(Integer var,OptimizeOpVisitorDPCaching visitor,CachingExecutor cachingExecutor) {
		this.var=var;
		scans = new ArrayList<IndexScan>();
		resultScans = new ArrayList<CachedResult>();
		this.visitor=visitor;
		this.cachingExecutor=cachingExecutor;
	}

	@Override
	public String print() {
		String ret = "{MergeJoin var:"+var+" centralized: "+
	    		centralized+" cost:"+cost+": \n";
		for(IndexScan s : scans){
			ret+=s.print()+"\n";
		}
		for(CachedResult s : resultScans){
			ret+=s.print()+"\n";
		}
		ret+="}";
		return ret;
	}

	@Override
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws Exception {

		MergeJoinPlan plan=new MergeJoinPlan();
		for(IndexScan scan: scans){
			plan.scans.add(scan.bgp);
		}
		for(CachedResult cr : resultScans){
			plan.resultScans.add(cr);
		}
		plan.maxPattern = maxScan;
		
		plan.maxPartition = maxPartition;
		plan.centalized = this.centralized;

		results = JoinExecutor.executeMerge1(plan, visitor.varIds.get(var), visitor.table,  visitor.indexTable, plan.centalized, visitor);
		
		CachedResult res = new CachedResult(results, results.get(0).stats,visitor);
		
	}

	public void add(DPJoinPlan plan) {
		if(plan.getClass().equals(CachedResult.class)){
			resultScans.add((CachedResult)plan);
		}
		else{
			scans.add((IndexScan)plan);
		}
	}

	public void setEdgeGraph(BitSet edges) {
		this.edges=edges;
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		return cost.compareTo(o.getCost());
	}

	public void computeCost() throws IOException {

    	n= Double.MAX_VALUE;
    	double sum_n2= 0;
    	int plength = 0;
    	Var v =visitor.varIds.get(var);
    	for(IndexScan scan : scans){
			long[][] p = scan.bgp.getPartitions(v);
			if(p.length>plength){
				maxPartition=p;
				plength=p.length;
				maxScan = scan;
			}
			double[] st = scan.getStatistics(var);
			//System.out.println("Stats var "+v+":"+ st[0]+" "+st[1]);
    		if(st[0]<n){
    			n=st[0];
    		}
    		sum_n2+=st[1];
    	}
		for(CachedResult s : resultScans){
			long[][] p = s.getPartitions(var);
			if(p.length>plength){
				maxPartition=p;
				plength=p.length;
				maxScan = s;
			}
			double[] st = s.getStatistics(var);
			if(st!=null){
				//System.out.println("Cachced stats var "+v+":"+ st[0]+" "+st[1]);
				if(st[0]<n){
					n=st[0];
				}
				sum_n2+=st[1];
			}
		}
    	
    	//compute cost
		double costCent = 0.0, costMR=MRoffset;
    	for(IndexScan scan : scans){
    		costCent+=MJcost(var, scan,n);
	    	costMR+=MJcost(var, scan,n)/visitor.workers;
    		//System.out.println("Costcent:"+costCent+" n:"+n);
    		//System.out.println("CostMR:"+costMR);
    	}
    	for(CachedResult s : resultScans){
	    	costCent+=MJcost(var, s,n);
    		costMR+=MJcost(var, s,n)/visitor.workers;
    	}
    	if(costCent< costMR){
    		cost= costCent;
    		centralized=true;
    	}
    	else{
    		cost=costMR;
    		centralized=false;
    	}
    	stats=new double[2];
    	stats[0]=n*sum_n2;
    	stats[1]=1;
    	
	}

	private double MJcost(Integer v, DPJoinPlan bgp,double n) throws IOException{
    	double[] stat = bgp.getStatistics(v);
		double readKeysSeek= n*(500.0+stat[1]);
		double readKeysScan=stat[0]*stat[1];
		double readKeys = (readKeysScan<readKeysSeek) ? readKeysScan : readKeysSeek;
		return readKeys/100000;
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
	public List<Integer> getOrdering() {
		List<Integer> ret = new ArrayList<Integer>();
		ret.add(var);
		return ret;
	}
	
}
