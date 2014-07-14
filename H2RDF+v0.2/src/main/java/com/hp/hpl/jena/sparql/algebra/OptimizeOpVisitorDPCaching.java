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

import gr.ntua.h2rdf.dpplanner.CacheController;
import gr.ntua.h2rdf.dpplanner.CachingExecutor;
import gr.ntua.h2rdf.dpplanner.DPJoinPlan;
import gr.ntua.h2rdf.dpplanner.DPSolver;
import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.PartitionFinder;
import gr.ntua.h2rdf.indexScans.ResultBGP;
import gr.ntua.h2rdf.loadTriples.ByteTriple;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.PatternVars;

public class OptimizeOpVisitorDPCaching extends OpVisitorBase {
	public Query query;
	public HashMap<Integer, Var> varIds;
	public HashMap<Var, Integer> varRevIds;
	public HashMap<Integer, Triple> tripleIds;
	public HashMap<Integer, BGP> bgpIds;
	public HashMap<Triple, Integer> tripleRevIds;
	public TreeMap<Integer, BitSet> varGraph;
	public TreeMap<Integer, TreeMap<Integer, BitSet>> edgeGraph;
	public PartitionFinder partitionFinder;
	
	//public Digraph<Integer> digraph, digraph2;
	public int numTriples, numVars;
	public CachingExecutor cachingExecutor;
	public HTable indexTable;
	public HTable table;
	public long statsTime,numStats;
	public final long selectivityOffset=1;//00;
	public final int workers=50;
	private List<ResultBGP> results;
	public boolean ordered, groupBy;
	public List<Var> orderVars, groupVars, projectionVars;
	public List<Integer> orderVarsInt;
	public BitSet selectiveIds;
	public boolean cacheRequests,cacheResults;
	private DPJoinPlan plan;
	
	
	public OptimizeOpVisitorDPCaching(Query query, CachingExecutor cachingExecutor, HTable table, HTable indexTable, PartitionFinder partitionFinder, boolean cacheRequests, boolean cacheResults) {
		this.partitionFinder =partitionFinder;
		this.cacheResults = cacheResults;
		this.cacheRequests = cacheRequests;
		this.query=query;
		this.cachingExecutor = cachingExecutor;
		this.table=table;
		this.indexTable=indexTable;
		ordered=query.isOrdered();
		groupBy = query.hasGroupBy();
		projectionVars = query.getProjectVars();
		orderVars= new ArrayList<Var>();
		groupVars= new ArrayList<Var>();
		if(query.isOrdered()){
			Iterator<SortCondition> it = query.getOrderBy().iterator();
			while(it.hasNext()){
				SortCondition cond = it.next();
				orderVars.add(cond.getExpression().asVar());
				System.out.println("Order By:"+cond.getExpression().asVar());
			}
		}
		orderVarsInt = new ArrayList<Integer>();
		if(groupBy){
			groupVars = query.getGroupBy().getVars();
		}
	}
	
	public void visit(OpOrder opOrder)
    {
		/*try {
			List<Var> orderVars = new ArrayList<Var>();
			
			for(SortCondition cond : opOrder.getConditions()){
				orderVars.add(cond.getExpression().getExprVar().asVar());
			}
			Integer firstOrderVar = varRevIds.get(orderVars.get(0));
			long[][] maxPartition = null;
			int plength=0;
			BitSet bgps = varGraph.get(firstOrderVar);
			for (Integer k = bgps.nextSetBit(0); k >= 0; k = bgps.nextSetBit(k+1)) {
				BGP bgp = bgpIds.get(k);
				long[][] p;
				p = bgp.getPartitions(orderVars.get(0));
				if(p.length>plength){
					maxPartition=p;
					plength=p.length;
				}
			}
			double size=0.0;
			ResultBGP result = results.get(0);
			for(Var v : result.joinVars){
				double[] stats = result.getStatistics(v,this);
				if(stats[0]>size){
					size=stats[0];
				}
			}
			size*=result.joinVars.size();
			System.out.println(size);
			
			for (int i = 0; i < maxPartition.length; i++) {
				System.out.print(maxPartition[i][1]+" ");
			}
			System.out.println();
			ResultBGP res = null;
			if(size<=2000000)
				res = JoinExecutor.executeOrdering(results.get(0), maxPartition, true);
			else
				res = JoinExecutor.executeOrdering(results.get(0), maxPartition, false);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    }

	public void visit(OpBGP opBGP)
    {
		try {
			
			//partitionFinder = new PartitionFinder(table.getStartEndKeys());
			
			varIds = new HashMap<Integer, Var>();
			varRevIds = new HashMap<Var, Integer>();
			tripleIds = new HashMap<Integer, Triple>();
			bgpIds = new HashMap<Integer, BGP>();
			tripleRevIds = new HashMap<Triple, Integer>();
			varGraph = new TreeMap<Integer, BitSet>();
			edgeGraph = new TreeMap<Integer, TreeMap<Integer,BitSet>>();
			
	    	System.out.println(opBGP.toString());
	    	
	    	Set<Var> vars = PatternVars.vars(query.getQueryPattern());
	    	Iterator<Var> it = vars.iterator();
	    	Integer id =0;
	    	numVars=0;
	    	while(it.hasNext()){
	    		Var v = it.next();
	    		varIds.put(id,v);
	    		varRevIds.put(v,id);
	    		id++;
	    		numVars++;
	    	}
	    	System.out.println(varIds);

			for(Var v: orderVars){
				orderVarsInt.add(varRevIds.get(v));
			}
			
	    	List<Triple> triples= opBGP.getPattern().getList();
			Iterator<Triple> it1 = triples.iterator();
	    	id =0;
	    	numTriples=0;
			while(it1.hasNext()){
				Triple t = it1.next();
    			BGP b = new BGP(t,table,indexTable, partitionFinder,this);
    			
    			//b.processSubclass();
    			bgpIds.put(id,b);
				tripleIds.put(id,t);
				tripleRevIds.put(t, id);
	    		id++;
	    		numTriples++;
			}
	    	System.out.println(tripleIds);
	    	vars = PatternVars.vars(query.getQueryPattern());
	    	it = vars.iterator();
	    	while(it.hasNext()){
	    		Var v = it.next();
	    		BitSet l = new BitSet(numVars);
	    		triples= opBGP.getPattern().getList();
	    		it1 = triples.iterator();
	    		while(it1.hasNext()){
	    			Triple t =it1.next();
	    			if(t.getSubject().equals(v) || t.getPredicate().equals(v) || t.getObject().equals(v)){
	    				l.set(tripleRevIds.get(t));
	    			}
	    		}
	    		varGraph.put(varRevIds.get(v), l);
	    	
	    	}
	    	
	    	System.out.println(varGraph);
	    	
	
			triples= opBGP.getPattern().getList();
			it1 = triples.iterator();
			while(it1.hasNext()){
				Triple t =it1.next();
				Integer tid=tripleRevIds.get(t);
				TreeMap<Integer, BitSet> map = new TreeMap<Integer, BitSet>();
				if(t.getSubject().isVariable()){
					Integer vid = varRevIds.get(t.getSubject());
					BitSet l = varGraph.get(vid);
					BitSet bn = new BitSet(numTriples);
					bn.or(l);
					bn.clear(tid);
					map.put(vid, bn);
				}
				if(t.getPredicate().isVariable()){
					Integer vid = varRevIds.get(t.getPredicate());
					BitSet l = varGraph.get(vid);
					BitSet bn = new BitSet(numTriples);
					bn.or(l);
					bn.clear(tid);
					map.put(vid, bn);
				}
				if(t.getObject().isVariable()){
					Integer vid = varRevIds.get(t.getObject());
					BitSet l = varGraph.get(vid);
					BitSet bn = new BitSet(numTriples);
					bn.or(l);
					bn.clear(tid);
					map.put(vid, bn);
				}
				edgeGraph.put(tid, map);
			}
	
	    	System.out.println(edgeGraph);
	    	
	    	selectiveIds = new BitSet(bgpIds.entrySet().size()*3);
	    	long max = 0;
	    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
	    		ByteTriple tr = e.getValue().byteTriples.get(0);
	    		int sid = e.getKey()*3;
	    		if(tr.getS()>=selectivityOffset){
	    			if(tr.getS()>max){
	    				max = tr.getS();
	    			}
	    		}
	    		if(tr.getO()>=selectivityOffset){
	    			if(tr.getO()>max){
	    				max = tr.getO();
	    			}
	    		}
	    	}
	    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
	    		ByteTriple tr = e.getValue().byteTriples.get(0);
	    		int sid = e.getKey()*3;
	    		if(tr.getS()>=selectivityOffset){
	    			if(max>10000 && tr.getS()>1000){
	    				selectiveIds.set(sid);
	    			}
	    			else if(max<800){
	    				selectiveIds.set(sid);
	    			}
	    		}
	    		//if(tr.getP()>=selectivityOffset){
	    		//	selectiveIds.set(sid+1);
	    		//}
	    		if(tr.getO()>=selectivityOffset){
	    			if(max>10000 && tr.getO()>1000){
	    				selectiveIds.set(sid+2);
	    			}
	    			else if(max<800){
	    				selectiveIds.set(sid+2);
	    			}
	    		}
	    	}
	    	System.out.println("SelectiveIds "+selectiveIds);
	    	/*digraph = new Digraph<Integer>();
	    	digraph2 = new Digraph<Integer>();//remove selective nodes
	    	//find vertex colors
	    	TreeSet<String> colorSet = new TreeSet<String>();
	    	TreeSet<String> colorSet2 = new TreeSet<String>();
	    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
	    		ByteTriple tr = e.getValue().byteTriples.get(0);
	    		String signature = tr.getS()+"_"+tr.getP()+"_"+tr.getO();
	    		String signature2 = getSignature2(tr);
	    		//System.out.println(signature+" "+signature2);
	    		
	    		colorSet.add(signature);
	    		colorSet2.add(signature2);
	    	}
	    	
	    	HashMap<String,Integer> vertexColors = new HashMap<String, Integer>();
	    	int i1 =0;
	    	for(String s : colorSet){
	    		vertexColors.put(s, i1);
	    		i1++;
	    	}
	    	HashMap<String,Integer> vertexColors2 = new HashMap<String, Integer>();
	    	i1 =0;
	    	for(String s : colorSet2){
	    		vertexColors2.put(s, i1);
	    		i1++;
	    	}
	    	
	    	//add vertices
	    	for(Entry<Integer, BGP> e : bgpIds.entrySet()){
	    		ByteTriple tr = e.getValue().byteTriples.get(0);
	    		String signature = tr.getS()+"_"+tr.getP()+"_"+tr.getO();
	    		Integer colorId = vertexColors.get(signature);
	    		digraph.add_vertex(e.getKey(), colorId);

	    		String signature2 = getSignature2(tr);
	    		Integer colorId2 = vertexColors2.get(signature2);
	    		digraph2.add_vertex(e.getKey(), colorId2);
	    	}
	    	int size = digraph.nof_vertices();
	    	//add 3 layers and connect them
	    	int colors = vertexColors.size(); 
	    	for (Integer i = 0; i < size; i++) {
				digraph.add_vertex(size+i,digraph.vertices.get(i).color+colors);
				digraph.add_edge(i, size+i);
				
				digraph2.add_vertex(size+i,digraph2.vertices.get(i).color+colors);
				digraph2.add_edge(i, size+i);
				//digraph.add_edge(size+i, i);
			}
	    	int s2=size+size;
	    	int c2=colors+colors;
	    	for (Integer i = 0; i < size; i++) {
				digraph.add_vertex(s2+i,digraph.vertices.get(i).color+c2);
				digraph.add_edge(size+i,s2+i);

				digraph2.add_vertex(s2+i,digraph2.vertices.get(i).color+c2);
				digraph2.add_edge(size+i,s2+i);
				//digraph.add_edge(s2+i, size+i);
			}
	    	
	    	//add edges to the respective layers
	    	for(Entry<Integer, TreeMap<Integer,BitSet>> e : edgeGraph.entrySet()){
	    		int nodeId=e.getKey();
	    		for (Entry<Integer,BitSet> edges:e.getValue().entrySet()) {
	    			BitSet bs = edges.getValue();
					for (int dest = bs.nextSetBit(0); dest >= 0; dest = bs.nextSetBit(dest+1)) {
						String s =bgpIds.get(nodeId).varPos.get(varIds.get(edges.getKey()).toString());
						String d =bgpIds.get(dest).varPos.get(varIds.get(edges.getKey()).toString());
						if(s.compareTo(d)>=0){
							addEdge(nodeId, dest,s+""+d,size);
							//System.out.println("Edge:"+nodeId+" "+dest+" : "+s+d);
						}
					}
				}
	    	}*/
	    	
	    	//digraph.write_dot(System.out);
	    	//digraph2.write_dot(System.out);
	    	
	    	statsTime=0;
	    	numStats=0;
	    	DPSolver solver = new DPSolver();

	    	long start = System.nanoTime();
	    	plan = solver.solve(this,cachingExecutor);

	    	long stop = System.nanoTime();
	    	System.out.println("Solve time us: "+(stop-start)/1000);
	    	System.out.println("Number of stats requests: "+numStats);
	    	System.out.println("Total stats time us: "+(statsTime)/1000);
	    	System.out.println(plan.print());
	
	    	plan.execute(this,cachingExecutor);

			double maxcov = (float)solver.maxCoverage/(float)numTriples;
			if(cacheRequests)
				CacheController.finishQuery(plan.getCost(),maxcov, this);
	    	results = plan.getResults();
	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
	
    }


	private String getSignature3(ByteTriple tr, BitSet bt, int i) {
		String ret = "";
		if(bt.get(i)){
			ret+="0_";
		}
		else{
			ret+=tr.getS()+"_";
		}
		if(bt.get(i+1)){
			ret+="0_";
		}
		else{
			ret+=tr.getP()+"_";
		}
		if(bt.get(i+2)){
			ret+="0_";
		}
		else{
			ret+=tr.getO()+"_";
		}
		return ret;
	}

	private String getSignature2(ByteTriple tr) {
		String ret = "";
		if(tr.getS()>=selectivityOffset){
			ret+="0_";
		}
		else{
			ret+=tr.getS()+"_";
		}
		if(tr.getP()>=selectivityOffset){
			ret+="0_";
		}
		else{
			ret+=tr.getP()+"_";
		}
		if(tr.getO()>=selectivityOffset){
			ret+="0_";
		}
		else{
			ret+=tr.getO()+"_";
		}
		return ret;
	}

/*	private void addEdge(int srcId, int destId, String type, int size) {
		int s2 = size+size;
		if(type.equals("ss")){//001
			digraph.add_edge(srcId, destId);
			
			digraph2.add_edge(srcId, destId);
		}
		else if(type.equals("sp")){//010
			digraph.add_edge(srcId+size, destId+size);
			
			digraph2.add_edge(srcId+size, destId+size);
		}
		else if(type.equals("so")){//011
			digraph.add_edge(srcId, destId);
			digraph.add_edge(srcId+size, destId+size);

			digraph2.add_edge(srcId, destId);
			digraph2.add_edge(srcId+size, destId+size);
		}
		else if(type.equals("pp")){//100
			digraph.add_edge(srcId+s2, destId+s2);

			digraph2.add_edge(srcId+s2, destId+s2);
		}
		else if(type.equals("po")){//101
			digraph.add_edge(srcId+s2, destId+s2);
			digraph.add_edge(srcId, destId);

			digraph2.add_edge(srcId+s2, destId+s2);
			digraph2.add_edge(srcId, destId);
		}
		else if(type.equals("oo")){//110
			digraph.add_edge(srcId+s2, destId+s2);
			digraph.add_edge(srcId+size, destId+size);
			

			digraph2.add_edge(srcId+s2, destId+s2);
			digraph2.add_edge(srcId+size, destId+size);
		}
	}
	*/
	public String getOutputFile() {
		/*"output/join_"+cachingExecutor.id+"_"+(cachingExecutor.tid-1);
		if(plan.equals(CachedResult.class))
			return (((CachedResult)(results.get(0))).results.get(0).path)+"";*/
		return results.get(0).path.toString();
	}
	
}
