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
package com.hp.hpl.jena.sparql.algebra;


import gr.ntua.h2rdf.dpplanner.IndexScan;
import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.JoinExecutor;
import gr.ntua.h2rdf.indexScans.MergeJoinPlan;
import gr.ntua.h2rdf.indexScans.PartitionFinder;
import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDatasetNames;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpTable;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.PatternVars;


public class OptimizeOpVisitorMergeJoin extends OpVisitorBase {

	private Query query;
	private String id;
	private OpBGP opBGP;
	private boolean ask;
	public static Map<Var, Byte> varIds=  new HashMap<Var, Byte>();
	public static boolean finished;
	private final int MRoffset=25;
	private HTable indexTable;
	private HTable table;
	
	public OptimizeOpVisitorMergeJoin(Query query, HTable t, HTable indexT) {
		this.table = t;
		this.indexTable = indexT;
		this.query = query;
		ask =true;
		varIds =  new HashMap<Var, Byte>();
		finished = false;
	}
	
    public void visit(OpBGP opBGP)
    {
		try {

			PartitionFinder partitionFinder = new PartitionFinder(table.getStartEndKeys());
	    	Set<Var> vars = PatternVars.vars(query.getQueryPattern());
	    	Iterator<Var> it = vars.iterator();
	    	byte id =0;
	    	while(it.hasNext()){
	    		Var v = it.next();
	    		varIds.put(v, id);
	    		id++;
	    	}
	    	//System.out.println("bgp");
	    	System.out.println(opBGP.toString());
	    	List<Triple> triples= opBGP.getPattern().getList();
	    	List<ResultBGP> tripleBGP = new ArrayList<ResultBGP>();
    		Iterator<Triple> it1 = triples.iterator();
    		while(it1.hasNext()){
    			Triple t = it1.next();
    			BGP b = new BGP(t,table,indexTable, partitionFinder,null);
    			b.processSubclass();
    			tripleBGP.add(b);
    		}
	    	
	    	vars = PatternVars.vars(query.getQueryPattern());
	    	HashMap<Var,List<ResultBGP>> map = new HashMap<Var, List<ResultBGP>>();
	    	
	    	it = vars.iterator();
	    	while(it.hasNext()){
	    		Var v = it.next();
	    		ArrayList<ResultBGP> temp = new ArrayList<ResultBGP>();
	    		Iterator<ResultBGP> it2 = tripleBGP.iterator();
	    		while(it2.hasNext()){
	    			BGP t = (BGP) it2.next();
	    			if(t.joinVars.contains(v))
	    			    temp.add(t);
	    			/*if(t.bgp.subjectMatches(v))
						temp.add(t);
	    			if(t.bgp.objectMatches(v))
						temp.add(t);
	    			if(t.bgp.predicateMatches(v))
						temp.add(t);*/
	    		}
	    		if(temp.size()>=2){
	    			map.put(v, temp);
	    		}
	    		else{
	    			for(ResultBGP b : temp){
	    				b.joinVars.remove(v);
	    			}
	    		}
	    		id++;
	    	}
	    	
	    	Set<Var> joinVars = new HashSet<Var>();
	    	joinVars.addAll(map.keySet());
	    	while(!joinVars.isEmpty()){

	    		MergeJoinPlan plan = null;
	    		it = joinVars.iterator();
		    		
	    		double minCost=Double.MAX_VALUE;
	    		double minCost1=Double.MAX_VALUE;
	    		Var joinVar = null;
	    		while(it.hasNext()){//greedy select joinVar
	    			Var v = it.next();
	    			int found = 0;
		    		for(ResultBGP t2 : map.get(v)){
		    			if(!t2.isJoined){
		    				found++;
		    				if(found>=2){
		    					break;
		    				}
		    			}
		    		}
		    		if(found<2){
			    		joinVars.remove(v);
		    			continue;
		    		}
	    			double cost=0;
	    			
	    			MergeJoinPlan pl =cJoin(v, map.get(v));
	    			
	    			cost+=pl.cost;
	    			if(minCost> cost){
	    				minCost1=pl.cost;
	    				minCost=cost;
	    				joinVar = v;
	    				if(plan==null)
	    					plan=new MergeJoinPlan();
	    				plan = pl;
	    			}
	    		}
	    		if(plan==null)
	    			continue;
	    		joinVars.remove(joinVar);
	    		for(BGP b : plan.scans){
	    			b.isJoined=true;
	    		}
	    		for(ResultBGP b : plan.intermediate){
	    			b.isJoined=true;
	    		}
	    		/*more joins
	    		 * 
	    		m.put(joinVar, plan);
	    		if(!plan.centalized){
	    			it = joinVars.iterator();
		    		while(it.hasNext()){//greedy select joinVar
		    			Var v = it.next();
		    			double cost=0;
		    			cost+= eCount(v, map.get(v), joinVars);
		    			JoinPlan pl =cJoin(v, map.get(v));
		    			cost+=pl.cost;
		    			if(pl.Map.size()+pl.Reduce.size()>=2){
			    			if(cost<=minCost1){
					    		for(ResultBGP b : pl.Map){
					    			b.isJoined=true;
					    		}
					    		for(ResultBGP b : pl.Reduce){
					    			b.isJoined=true;
					    		}
					    		m.put(v, pl);
			    			}
		    			}
		    		}
	    		}
	    		*/
	    		List<ResultBGP> result = JoinExecutor.executeMerge(plan, joinVar, table, indexTable, plan.centalized);
	    		if(finished)
	    			return;
	    		for(ResultBGP res : result){
		    		for(Var v : res.joinVars){
		    			map.get(v);
		    			map.get(v).add(res);
		    		}
	    		}
    			
	    	}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }

    

	private MergeJoinPlan cJoin(Var v, List<ResultBGP> list) throws IOException {
		MergeJoinPlan retPlan= new MergeJoinPlan();
    	Iterator<ResultBGP> it = list.iterator();
    	double n= Double.MAX_VALUE;
    	long[][] maxPartition=null;
    	int plength = 0;
    	BGP maxBGP=null;
    	while(it.hasNext()){
    		ResultBGP temp = it.next();
    		if(temp.isJoined)
    			continue;
    		if(temp.getClass().equals(BGP.class)){
    			BGP e = (BGP)temp;
    			retPlan.scans.add(e);
    			long[][] p = e.getPartitions(v);
    			if(p.length>plength){
    				maxPartition=p;
    				plength=p.length;
    				maxBGP = e;
    			}
    			double[] ret = temp.getStatistics(v);
        		if(ret[0]<n){
        			n=ret[0];
        		}
    		}
    		else{
    			retPlan.intermediate.add(temp);
    			double[] ret = temp.getStatistics(v);
        		if(ret[0]<n){
        			n=ret[0];
        		}
    		}
    	}
    	retPlan.maxPattern = new IndexScan(maxBGP.bgp, maxBGP, null, null);
    	retPlan.maxPartition = maxPartition;
    	//compute cost
		double costCent = 0, costMR=MRoffset;
    	for(BGP e : retPlan.scans){
    		costCent+=Mc(v, e,n);
    		costMR+=MT(v, e,n,plength);
    	}
    	for(ResultBGP e : retPlan.intermediate){
    		costCent+=Mc(v, e,n);
    		costMR+=MT(v, e,n,plength);
    	}
    	if(costCent< costMR){
    		retPlan.centalized = true;
    		retPlan.cost=costCent;
    		return retPlan;
    	}
    	else{
    		retPlan.centalized = false;
    		retPlan.cost=costMR;
    		return retPlan;
    	}
	}
    
    private double MT(Var v, ResultBGP bgp,double n, int plength) throws IOException {
    	double[] stat = bgp.getStatistics(v);
    	double mapsplits = plength;
    	double mappers = 10;
    	double kpm = n*stat[1]/mappers;
    	//System.out.println("n: "+n*stat[1]+" sec: "+kpm*0.00001);
    	return kpm*0.0001+3;
	}

	private double MT(Var v, ResultBGP bgp, double n) throws IOException {
    	double[] stat = bgp.getStatistics(v);
    	double regsize = 1000000;
    	double mapsplits = Math.ceil(n*stat[1]/regsize)*2;
    	double mappers = 10;
    	return mapsplits/mappers*15;
	}

	private double Mc(Var v, ResultBGP bgp, double n) throws IOException{
    	double[] stat = bgp.getStatistics(v);
    	//System.out.println(stat[0]+" "+stat[1]);
    	//if(n*stat[1]>=10000){
    		return n*stat[1]*0.0001+3;
    	//}
    	//else{
    	//	return 0.7;
    	//}
    }
    
    private double Rc(Var v, ResultBGP bgp, double threads) throws IOException{
    	double[] stat = bgp.getStatistics(v);
    	//double N = Math.pow(stat[1], 2);
    	//double ret = (0.000025*N+0.15)*threads + (0.000059*N);
    	double N = stat[1];
    	double ret = 0.3 + (0.0005*N);
    	//System.out.println(N+" "+ret);
    	return ret;
    }

	public void visit(OpJoin opJoin)
    {
    	System.out.println("join");
    	System.out.println(opJoin.toString());}

    public void visit(OpLeftJoin opLeftJoin)
    {
    	System.out.println("left join");
    	System.out.println(opLeftJoin.toString());}

    public void visit(OpUnion opUnion)
    {
    	System.out.println("union");
    	System.out.println(opUnion.toString());}

    public void visit(OpFilter opFilter)
    {
    	System.out.println("filter");
    	System.out.println(opFilter.toString());
    	/*//JoinPlanner.setFilterVars();
    	Iterator<Expr> it = opFilter.getExprs().iterator();
    	while(it.hasNext()){
    		Expr e =it.next();
    		Iterator<Expr> a = e.getFunction().getArgs().iterator();
			System.out.println(e.getFunction().getOpName());
    		while(a.hasNext()){
    			Expr temp = a.next();
    			if(temp.isVariable())
    				JoinPlaner.filter(temp.toString(),e.getFunction());
    		}
    	}*/
    }

    public void visit(OpGraph opGraph)
    {
    	System.out.println("graph");
    	System.out.println(opGraph.toString());}

    public void visit(OpQuadPattern quadPattern)
    {
    	System.out.println("quad");
    	System.out.println(quadPattern.toString());}

    public void visit(OpDatasetNames dsNames)
    {
    	System.out.println("dsNames");
    	System.out.println(dsNames.toString());}

    public void visit(OpTable table)
    {
    	System.out.println("table");
    	System.out.println(table.toString());}

    public void visit(OpExt opExt)
    {
    	System.out.println("ext");
    	System.out.println(opExt.toString());}

    public void visit(OpOrder opOrder)
    {
    	System.out.println("order");
    	System.out.println(opOrder.toString());}

    public void visit(OpProject opProject)
    {
    	//System.out.println("project");
    	//System.out.println(opProject.toString());
    	ask = false;
    }

    public void visit(OpDistinct opDistinct)
    {
    	System.out.println("distinct");
    	System.out.println(opDistinct.toString());}

    public void visit(OpSlice opSlice)
    {
    	System.out.println("slice");
    	System.out.println(opSlice.toString());}
    
    
}
