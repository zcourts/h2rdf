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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class SortMergeJoin implements DPJoinPlan{
	private Integer var;
	private List<IndexScan> scans;
	private List<CachedResult> resultScans;
	private List<DPJoinPlan> joinResults;
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
	
	public SortMergeJoin(Integer var,OptimizeOpVisitorDPCaching visitor,CachingExecutor cachingExecutor) {
		this.var=var;
		scans = new ArrayList<IndexScan>();
		resultScans = new ArrayList<CachedResult>();
		joinResults = new ArrayList<DPJoinPlan>();
		this.visitor=visitor;
		this.cachingExecutor=cachingExecutor;
	}

	@Override
	public String print() {
		String ret = "{SortMergeJoin var:"+var+" centralized: "+
	    		centralized+" cost:"+cost+": \nScans: \n";
		for(IndexScan s : scans){
			ret+=s.print()+"\n";
		}
		for(CachedResult s : resultScans){
			ret+=s.print()+"\n";
		}
		ret+="Joins: \n";
		for(DPJoinPlan s : joinResults){
			ret+=s.print()+"\n";
		}
		ret+="}";
		return ret;
		
	}


	public void addScan(DPJoinPlan plan) {
		if(plan.getClass().equals(CachedResult.class)){
			resultScans.add((CachedResult)plan);
		}
		else{
			scans.add((IndexScan)plan);
		}
	}

	public void addJoin(DPJoinPlan plan) {
		joinResults.add(plan);
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
    	for(DPJoinPlan pl : joinResults){
    		double[] st = pl.getStatistics(var);
			//System.out.println("Stats var "+v+":"+ st[0]+" "+st[1]);
			if(st[0]<n){
    			n=st[0];
			}
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
		
		if(maxPartition==null){
			for(BGP b : visitor.bgpIds.values()){
				if(b.joinVars.contains(v)){
					long[][] p = b.getPartitions(v);
					if(p.length>plength){
						maxPartition=p;
						plength=p.length;
					}
				}
			}
		}
    	//cost=10.0*scans.size();
    	cost=0.0;
		//compute cost
    	double costCent = 0, costMR=MRoffset;
    	for(IndexScan scan : scans){
	    	costCent+=MJcost(var, scan,n);
    		costMR+=MJcost(var, scan,n)/visitor.workers;
    		//System.out.println("Costcent:"+costCent+" n:"+n);
    	}
    	for(CachedResult s : resultScans){
	    	costCent+=MJcost(var, s,n);
    		costMR+=MJcost(var, s,n)/visitor.workers;
    	}
    	for(DPJoinPlan pl : joinResults){
    		cost+=pl.getCost();
			double[] st = pl.getStatistics(var);
			//System.out.println("Stats var "+v+":"+ st[0]+" "+st[1]);
    		sum_n2+=st[0]/n;
    		//System.out.println("Costcent:"+costCent+" n:"+n);
    		
	    	costCent+=SMJcost(var, pl,n);
	    	costMR+=SMJcost(var, pl,n)/visitor.workers;
    	}
    	if(costCent< costMR){
    		cost+= costCent;
    		centralized=true;
    	}
    	else{
    		cost+=costMR;
    		centralized=false;
    	}
    	stats=new double[2];
    	stats[0]=n*sum_n2;
    	stats[1]=1;
	}
	
	private double MJcost(Integer v, DPJoinPlan bgp,double n) throws IOException{
    	double[] stat = bgp.getStatistics(v);
		//double readKeysSeek=n*stat[1]*1000.0;
		double readKeysSeek= n*(500.0+stat[1]);
		double readKeysScan=stat[0]*stat[1];
		double readKeys = (readKeysScan<readKeysSeek) ? readKeysScan : readKeysSeek;
		return readKeys/100000;
	}
	
	private double SMJcost(Integer v, DPJoinPlan bgp,double n) throws IOException{
    	double[] stat = bgp.getStatistics(v);
		return stat[0]*stat[1]*2/100000;
	}
	
	@Override
	public Double getCost() {
		return cost;
	}
	
	@Override
	public List<Integer> getOrdering() {
		List<Integer> ret = new ArrayList<Integer>();
		ret.add(var);
		return ret;
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
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws Exception {
		for(DPJoinPlan s : joinResults){
			s.execute(visitor, cachingExecutor);
		}
		int mergePartSize=0;
		MergeJoinPlan plan=new MergeJoinPlan();
		List<Long> partition= new ArrayList<Long>();
		for(IndexScan scan: scans){
			plan.scans.add(scan.bgp);
			long[][] l = scan.bgp.getPartitions(visitor.varIds.get(var));
			for (int i = 1; i < l.length; i++) {
				partition.add(l[i][0]);
			}
			mergePartSize++;
		}
			
		for(CachedResult cr : resultScans){
			plan.resultScans.add(cr);
		}
		for(DPJoinPlan scan: joinResults){
			for (ResultBGP res : scan.getResults()) {
				if(scan.getClass().equals(CachedResult.class)){
					if(((CachedResult)scan).selectiveBindings!=null){
						res.selectiveBindings=((CachedResult)scan).selectiveBindings;
					}
					if(!((CachedResult)scan).ordered){
						plan.intermediate.add(res);
					}
					else{
						//to do!!
					}
				}
				else{
					plan.intermediate.add(res);
					if(res.partitions!=null){
						long[][] l = res.partitions.get(var);
						if(l!=null){
							for (int i = 1; i < l.length; i++) {
								partition.add(l[i][0]);
							}
							mergePartSize++;
						}
					}
				}
			}
		}
		Collections.sort(partition);
		int i =0;
		List<Long> finalPart1 = new ArrayList<Long>();
		while(i<partition.size()){
			finalPart1.add(partition.get(i));
			i+=mergePartSize;
		}
		long[][] part = new long[finalPart1.size()+1][1];
		i =1;
		part[0][0]=0;
		for(Long l : finalPart1){
			part[i][0]=l;
			i++;
		}
		
		
		plan.maxPattern = maxScan;
		plan.maxPartition = part;
		if(finalPart1.isEmpty()){
			plan.centalized = true;
		}
		else{
			plan.centalized = this.centralized;
		}
		results = JoinExecutor.executeMerge1(plan, visitor.varIds.get(var), visitor.table,  visitor.indexTable, plan.centalized, visitor);

		CachedResult res = new CachedResult(results, results.get(0).stats,visitor);

	}
	
}



