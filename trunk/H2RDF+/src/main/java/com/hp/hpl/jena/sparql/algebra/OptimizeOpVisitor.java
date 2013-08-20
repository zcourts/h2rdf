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
package com.hp.hpl.jena.sparql.algebra;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.conf.Configuration;

import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.JoinExecutor;
import gr.ntua.h2rdf.indexScans.JoinPlan;
import gr.ntua.h2rdf.indexScans.ResultBGP;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.op.*;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.pfunction.library.concat;
import com.hp.hpl.jena.sparql.syntax.PatternVars;


public class OptimizeOpVisitor extends OpVisitorBase {

	private Query query;
	private String id;
	private OpBGP opBGP;
	private boolean ask;
	public static Map<Var, Byte> varIds;
	private final int MRoffset=35;
	private HTable indexTable;
	private HTable table;
	
	public OptimizeOpVisitor(Query query, HTable t, HTable indexT) {
		this.table = t;
		this.indexTable = indexT;
		this.query = query;
		ask =true;
		varIds =  new HashMap<Var, Byte>();
	}
	
    public void visit(OpBGP opBGP)
    {
		try {

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
    			BGP b = new BGP(t,table,indexTable, null);
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
	    	
	    	Set<Var> joinVars = map.keySet();
    		Set<Var> nextjoinVars = null;
	    	while(!joinVars.isEmpty()){

		    	Map<Var,JoinPlan> m = new HashMap<Var, JoinPlan>();
		    	if(nextjoinVars!=null){
		    		joinVars=nextjoinVars;
		    	}
	    		nextjoinVars = new HashSet<Var>();
	    		it = joinVars.iterator();
		    		
	    		double minCost=Double.MAX_VALUE;
	    		double minCost1=Double.MAX_VALUE;
	    		Var joinVar = null;
	    		JoinPlan plan = null;
	    		while(it.hasNext()){//greedy select joinVar
	    			Var v = it.next();
	    			int found = 0;
		    		for(ResultBGP t2 : map.get(v)){
		    			if(!t2.isJoined){
		    				found++;
		    				if(found>=2){
		    					nextjoinVars.add(v);
		    					break;
		    				}
		    			}
		    		}
		    		if(found<2){
		    			continue;
		    		}
	    			double cost=0;
	    			cost+= eCount(v, map.get(v), joinVars);
	    			JoinPlan pl =cJoin(v, map.get(v));
	    			cost+=pl.cost;
	    			if(minCost> cost){
	    				minCost1=pl.cost;
	    				minCost=cost;
	    				joinVar = v;
	    				plan = pl;
	    			}
	    		}
	    		if(plan==null)
	    			continue;
	    		//joinVars.remove(joinVar);
	    		for(ResultBGP b : plan.Map){
	    			b.isJoined=true;
	    		}
	    		for(ResultBGP b : plan.Reduce){
	    			b.isJoined=true;
	    		}
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
	    		List<ResultBGP> result = JoinExecutor.execute(m, table, indexTable, plan.centalized);
	    		for(ResultBGP res : result){
		    		for(Var v : res.joinVars){
		    			map.get(v).add(res);
		    		}
	    		}
    			
	    	}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }

    

	private JoinPlan cJoin(Var v, List<ResultBGP> list) throws IOException {
    	JoinPlan retPlan= new JoinPlan();
    	Iterator<ResultBGP> it = list.iterator();
    	double n= Double.MAX_VALUE;
    	while(it.hasNext()){
    		ResultBGP temp = it.next();
    		if(temp.isJoined)
    			continue;
    		double[] ret = temp.getStatistics(v);
    		if(ret[0]<n){
    			n=ret[0];
    		}
    	}
    	it = list.iterator();
		double threads = n/20;
		if(threads>= 40)
			threads=40;
		threads = (int) Math.round(threads)+1;
		double costCent = 0, costMR=MRoffset;
		Set<ResultBGP> MCent = new HashSet<ResultBGP>();
		Set<ResultBGP> RCent = new HashSet<ResultBGP>();
		Set<ResultBGP> Mmr = new HashSet<ResultBGP>();
		Set<ResultBGP> Rmr = new HashSet<ResultBGP>();
		double minMCent= Double.MAX_VALUE;
		double minMMR= Double.MAX_VALUE;
		ResultBGP minCent, minMR;
    	while(it.hasNext()){
    		ResultBGP temp = it.next();
    		if(temp.isJoined)
    			continue;
    		//System.out.println("map"+Mc(v,temp));
    		//System.out.println("reduce"+n/threads);
        	double mc = Mc(v,temp);
        	double rc = n/threads*Rc(v, temp, threads);
        	//System.out.println("mc: "+mc);
        	//System.out.println("threads: "+threads);
        	//System.out.println("rc: "+rc);
        	if(mc<minMCent){
        		minMCent=mc;
        		minCent = temp;
        	}
			if(mc<rc){
				MCent.add(temp);
    			costCent+=mc;
    		}
			else{
				RCent.add(temp);
    			costCent+=rc;
			}
	    	double reducers = 10;
        	double mt = MT(v,temp);
        	double rt = n/reducers*Rc(v, temp, reducers);
        	if(mt<rt){
        		Mmr.add(temp);
        		costMR+=mt;
        	}
        	else{
        		Rmr.add(temp);
        		costMR+=rt;
        	}
        	if(mt<minMMR){
        		minMMR=mt;
        		minMR = temp;
        	}
    	}
    	//System.out.println(costCent);
    	if(costCent<= costMR){
    		retPlan.centalized = true;
    		retPlan.cost = costCent;
    		retPlan.Map = MCent;
    		retPlan.Reduce = RCent;
    		return retPlan;
    	}
    	else{
    		retPlan.centalized = false;
    		retPlan.cost = costMR;
    		retPlan.Map = Mmr;
    		retPlan.Reduce = Rmr;
    		return retPlan;
    	}
	}
    
    private double MT(Var v, ResultBGP bgp) throws IOException {
    	double[] stat = bgp.getStatistics(v);
    	double regsize = 1000000;
    	double mapsplits = Math.ceil(stat[0]*stat[1]/regsize)*2;
    	double mappers = 10;
    	return mapsplits/mappers*15;
	}

	private double Mc(Var v, ResultBGP bgp) throws IOException{
    	double[] stat = bgp.getStatistics(v);
    	//System.out.println(stat[0]+" "+stat[1]);
    	if(stat[0]*stat[1]>=10000){
    		return stat[0]*stat[1]*0.000231 -0.729951;
    	}
    	else{
    		return 0.7;
    	}
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

	private double eCount(Var v, List<ResultBGP> triples, Set<Var> joinVars) {
    	double ret =0;
    	Iterator<ResultBGP> it = triples.iterator();
    	Set<Var> otherVars = new HashSet<Var>();
    	while(it.hasNext()){
    		ResultBGP tr = it.next();
    		if(tr.isJoined)
    			continue;
    		Iterator<Var> it1 = joinVars.iterator();
    		while(it1.hasNext()){
    			Var temp = it1.next();
    			if(temp==v)
    				continue;
    			if(tr.joinVars.contains(temp))
					otherVars.add(temp);
    			
    			
    		}
    	}
    	ret+=otherVars.size()*MRoffset;
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
