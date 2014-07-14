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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

public class DPSolver {
	OptimizeOpVisitorDPCaching visitor;
	CachingExecutor cachingExecutor;
	private int n,totalChecks, cacheChecks;
	private HashMap<BitSet,DPJoinPlan>dptable =new HashMap<BitSet, DPJoinPlan>();
	public int maxCoverage;
	
	public DPJoinPlan solve(OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor) throws IOException {
		this.visitor=visitor;
		this.cachingExecutor=cachingExecutor;
		if(visitor.cacheRequests)
			CacheController.newQuery();
		totalChecks=0;
		cacheChecks=0;
		n=visitor.numTriples;
		maxCoverage=0;
		
		for(Integer v : visitor.edgeGraph.descendingKeySet()){
			BitSet b = new BitSet(n);
			b.set(v);
			//System.out.println(b+ " scan "+v);
			dptable.put(b, new IndexScan(visitor.tripleIds.get(v),visitor.bgpIds.get(v),visitor,cachingExecutor));
		}
		
		for(Integer v : visitor.edgeGraph.descendingKeySet()){
			BitSet b = new BitSet(n);
			b.set(v);
			emitCsg(b);
			BitSet bv = new BitSet(n);
			for (int i = 0; i <= v; i++) {
				bv.set(i);
			}
			enumerateCsgRec(b,bv);
		}
		BitSet b = new BitSet(n);
		for (int i = 0; i < n; i++) {
			b.set(i);
		}
		System.out.println("Cache Checks: "+cacheChecks);
		System.out.println("Join Checks: "+totalChecks);
		//System.out.println("Optimal: "+ dptable.get(b));
		
		
		return dptable.get(b);
	}

	private void enumerateCsgRec(BitSet b, BitSet bv) throws IOException {
		//System.out.println("EnumerateCsgRec S1: "+b+" X: "+bv);
		BitSet N = neighbor(b,bv);
		//System.out.println("N: "+N);
		if(N.isEmpty())
			return;
		PowerSet pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(b);
        		//System.out.println("Check DPtable: "+t);
        		//if(dptable.containsKey(t)){
            		emitCsg(t);
        		//}
        	}
        }
		
        pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(b);
        		BitSet Xnew = new BitSet(n);
        		Xnew.or(bv);
        		Xnew.or(N);
        		enumerateCsgRec(t,Xnew);
        	}
        }
		
	}



	private void emitCsg(BitSet s1) throws IOException {
		System.out.println("emitCsg s:"+s1);
		
		//check cache!!!!!
		if(s1.cardinality()>=2){
			System.out.println("Check cache s:"+s1);
			DPJoinPlan r = null;
			DPJoinPlan val = dptable.get(s1);
			Double c=1.0;
			if(val!=null)
				c=val.getCost();
			CachedResults res =null;//CanonicalLabel.checkCache2(s1, visitor,cachingExecutor,c);
			cacheChecks++;
			if(res==null){
				//res =CanonicalLabel.checkCacheNoSelective(s1, visitor,cachingExecutor);
				//if(res==null){
					System.out.println("Not found in cache: ");
				//}
				//cacheChecks++;
				//res.add
			}
			if(res!=null){
				System.out.println("Found in cache: "+res.print());
				if(s1.cardinality()>maxCoverage)
					maxCoverage=s1.cardinality();
				res.computeCost();
				DPJoinPlan p = res;
				if(s1.cardinality()==n){//final join add order and group by
					if(visitor.groupBy){
						GroupBy g = new GroupBy(visitor.groupVars, visitor, cachingExecutor);
						g.setEdgeGraph(s1);
						g.subplan = p;
						p=g;
						p.computeCost();
					}
					if(visitor.ordered){
						OrderBy o = new OrderBy(visitor.orderVars, visitor, cachingExecutor);
						CachedResult temp = res.getResultWithOrdering(o.getOrdering());
						if(temp!=null){
							p=temp;
						}
						else{
							o.setEdgeGraph(s1);
							o.subplan = p;
							p=o;
							p.computeCost();
						}
					}
				}
				if(val==null || p.compareTo(val)<0){
					dptable.put(s1, p);
				}
			}
		}
		//if(s1.get(3)&&s1.get(5)&&!s1.get(2)&&!s1.get(1))
		//	dptable.put(s1, "{"+s1+" from cache }");
		//if(cacheChecks%20==0)
			//dptable.put(s1, " ");
		
		//System.out.println("EmitCsg S1: "+s1);
		BitSet X = new BitSet(n);
		int mins1=s1.nextSetBit(0);
		for (int i = 0; i < mins1; i++) {
			X.set(i);
		}
		X.or(s1);
		
		//System.out.println("X: "+ X);
		HashMap<Integer, BitSet> map = neighbor2(s1,X);//multi-way and two-way joins
		//HashMap<Integer, BitSet> map = neighborhood2(s1,X);//only full multi-way joins
		//System.out.println("Neighbor: "+map);
		//multiway join enumeration
		/*for(Integer var :map.keySet()){
			BitSet N = map.get(var);
    		BitSet Xn = new BitSet(n);
            Xn.or(X);
            Xn.or(N);
			List<BitSet> l = new ArrayList<BitSet>();
			l.add(s1);
			BitSet tempN=new BitSet(n);
			tempN.or(N);
			l.add(tempN);
			//System.out.println("Enumerate multi-way join "+ var+": "+l +" X:"+Xn);
			enumerateCsgList(l,1,Xn,var);
			for (int i = 0; i <N.size() ; i++) {
				if(N.get(i)){
					tempN.clear(i);
					BitSet temp = new BitSet(n);
					temp.set(i);
					l.remove(l.size()-1);
					l.add(temp);
					if(!tempN.isEmpty())
						l.add(tempN);
					else
						break;
					
					//System.out.println("Enumerate multi-way join "+ var+": "+l +" X:"+Xn);
					enumerateCsgList(l,1,Xn, var);
				}
			}
			
		}*/
		for(Integer var :map.keySet()){
			BitSet s2 = map.get(var);
    		BitSet Xn = new BitSet(n);
            Xn.or(X);
            Xn.or(s2);
			List<BitSet> l = new ArrayList<BitSet>();
			l.add(s1);
			BitSet s2N=(BitSet)s2.clone();
			//s2N.or(s2);
			//System.out.println("Enumerate multi-way join "+ var+": "+l +" X:"+Xn);
			enumerateCmpLcList(l,s2N,Xn,var);
		}
	}
	

	private void enumerateCmpLcList(List<BitSet> l, BitSet S2, BitSet X, Integer var) throws IOException {
		if(S2.cardinality()==0){
			extendCmpLcList(l, 1, X, var);
			return;
		}
		int min = S2.nextSetBit(0);
		S2.clear(min);
		PowerSet p = new PowerSet(S2);
		for(BitSet b : p){
			b.set(min);
			BitSet St =(BitSet) S2.clone();
			St.andNot(b);
			l.add(b);
			enumerateCmpLcList(l,St,X,var);
			l.remove(l.size()-1);
		}
	}

	private void extendCmpLcList(List<BitSet> l, int i, BitSet X, Integer var) throws IOException {
		//System.out.println("Extend: "+l+" i:"+i );
		if(i<l.size()){
			extendCmpLcList(l, i + 1, X, var);
		}
		else{
			emitCsgList(l, var);
			return;
		}
		BitSet S = l.remove(i);
		BitSet N = neighbor(S,X);
		//System.out.println("N: "+N);
		if(N.isEmpty()){
    		l.add(i, S);
			return;
		}

		BitSet Xn = new BitSet(n);
		Xn.or(X);
		Xn.or(N);
		PowerSet pset = new PowerSet(N);
        for(BitSet S1:pset)
        {
        	if(!S1.isEmpty()){
        		S1.or(S);
        		l.add(i, S1);
        		extendCmpLcList(l, i + 1, Xn, var);
        		l.remove(i);
        	}
        }
		l.add(i, S);
	}

	private void enumerateCsgList(List<BitSet> l, int index, BitSet X, Integer var) throws IOException {
		//System.out.println("Enumerate: "+l +" index: "+index+" X:"+X);
		if(index<l.size()-1)
			enumerateCsgList(l,index+1,X, var);
		else
			emitCsgList(l, var);
			
		BitSet en = l.get(index);
		BitSet N = neighbor(en,X);
		if(N.isEmpty())
			return;
		PowerSet pset = new PowerSet(N);

		BitSet Xn = new BitSet(n);
        Xn.or(X);
        Xn.or(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(en);
        		//System.out.println("Check DPtable: "+t);
        		if(dptable.containsKey(t)){
        			List<BitSet> ln = new ArrayList<BitSet>();
        			for (int i = 0; i < l.size(); i++) {
        				if(i==index){
        					ln.add(index, t);
        				}
        				else{
        					ln.add(i, l.get(i));
        				}
						
					}
        			
        			enumerateCsgList(ln,index,Xn,var);
        		}
        	}
        }
		
		
		
	}

	private void emitCsgList(List<BitSet> l, Integer var) throws IOException {
		System.out.println("Emit multi-way join "+var+": "+l);
		totalChecks++;
		BitSet s = new BitSet(n);
		int scans=0, joinResults=0, sum=0;
		
		MergeJoin m = new MergeJoin(var, visitor, cachingExecutor);
		SortMergeJoin sm = new SortMergeJoin(var, visitor, cachingExecutor);
		
		for(BitSet b : l){
			DPJoinPlan plan = dptable.get(b);
			if(plan==null)
				return;
			if(plan.getClass().equals(IndexScan.class)){
				scans++;
				m.add(plan);
				sm.addScan(plan);
			}
			else if(plan.getClass().equals(CachedResults.class)){
				CachedResults c = (CachedResults)plan;
				List<Integer> t = new ArrayList<Integer>();
				t.add(var);
				CachedResult cr = c.getResultWithOrdering(t);
				if(cr==null){
					joinResults++;
					sm.addJoin(c.results.get(0));
				}
				else{
					scans++;
					m.add(cr);
					sm.addScan(cr);
				}
			}
			else{
				joinResults++;
				sm.addJoin(plan);
			}
			sum++;
			s.or(b);
		}
		
		DPJoinPlan prevPlan = dptable.get(s);
		if(sum==scans){
			m.setEdgeGraph(s);
			m.computeCost();
			DPJoinPlan p = m;
			if(s.cardinality()==n){//final join add order and group by
				if(visitor.groupBy){
					GroupBy g = new GroupBy(visitor.groupVars, visitor, cachingExecutor);
					g.setEdgeGraph(s);
					g.subplan = p;
					p=g;
					p.computeCost();
				}
				if(visitor.ordered){
					OrderBy o = new OrderBy(visitor.orderVars, visitor, cachingExecutor);
					o.setEdgeGraph(s);
					o.subplan = p;
					p=o;
					p.computeCost();
				}
			}
			
			//System.out.println(m.print(visitor, cachingExecutor));
			if(prevPlan==null){
				dptable.put(s, p);
			}
			else{
				if(m.compareTo(prevPlan)<0){
					dptable.put(s, p);
				}
			}
		}
		else{
			sm.setEdgeGraph(s);
			sm.computeCost();
			DPJoinPlan p = sm;
			if(s.cardinality()==n){//final join add order and group by
				if(visitor.groupBy){
					GroupBy g = new GroupBy(visitor.groupVars, visitor, cachingExecutor);
					g.setEdgeGraph(s);
					g.subplan = p;
					p=g;
					p.computeCost();
				}
				if(visitor.ordered){
					OrderBy o = new OrderBy(visitor.orderVars, visitor, cachingExecutor);
					o.setEdgeGraph(s);
					o.subplan = p;
					o.computeCost();
					p=o;
				}
				
			}
			//System.out.println(sm.print(visitor, cachingExecutor));
			if(prevPlan==null){
				dptable.put(s, p);
			}
			else{
				if(p.compareTo(prevPlan)<0){
					dptable.put(s, p);
				}
			}
		}
	}


	public BitSet neighbor(BitSet S, BitSet X) {
		BitSet N = new BitSet(n);
		for (int i = 0; i < n; i++) {
			if(S.get(i)){
				for(BitSet s : visitor.edgeGraph.get(i).values()){
					N.or(s);
				}
			}
		}
		N.andNot(X);
		return N;
	}
	
	private HashMap<Integer, BitSet> neighborhood2(BitSet S, BitSet X) {
		HashMap<Integer,BitSet> map = new HashMap<Integer, BitSet>();
		for (int i = 0; i < n; i++) {
			if(S.get(i)){
				for(Integer v : visitor.edgeGraph.get(i).keySet()){
					BitSet entry = map.get(v);
					if(entry==null){
						entry = new BitSet(n);
						entry.or(visitor.edgeGraph.get(i).get(v));
						entry.andNot(X);
						if(entry.cardinality()>0)
							map.put(v, entry);
					}
					else{
						entry.or(visitor.edgeGraph.get(i).get(v));
						entry.andNot(X);
						if(entry.cardinality()>0)
							map.put(v, entry);
					}
				}
				
			}
		}
		return map;
	}
	
	private HashMap<Integer, BitSet> neighbor2(BitSet S, BitSet X) {
		//System.out.println("Neighborhood: "+S+" exclusion: "+X);
		//get neighdorhood per var
		HashMap<Integer,BitSet> map = new HashMap<Integer, BitSet>();
		for (int i = 0; i < n; i++) {
			if(S.get(i)){
				for(Integer v : visitor.edgeGraph.get(i).keySet()){
					BitSet entry = map.get(v);
					if(entry==null){
						entry = new BitSet(n);
						entry.or(visitor.edgeGraph.get(i).get(v));
						entry.andNot(X);
						map.put(v, entry);
					}
					else{
						entry.or(visitor.edgeGraph.get(i).get(v));
						entry.andNot(X);
						map.put(v, entry);
					}
				}
				
			}
		}
		//check if U S contains all triples for var
		HashMap<Integer,BitSet> ret = new HashMap<Integer, BitSet>();
		for(Integer i : map.keySet()){
			BitSet val = map.get(i);
			BitSet valn = new BitSet(n);
			valn.or(val);
			valn.or(S);
			BitSet varBit = visitor.varGraph.get(i);
			int s=0, s1=0;
			for (int k = varBit.nextSetBit(0); k >= 0; k = varBit.nextSetBit(k+1)) {
				if(valn.get(k)){
					s1++;
				}
				s++;
			}
			if(s==s1){
				if(!val.isEmpty()){
					ret.put(i, val);
				}
			}
		}
		return ret;
	}
	
}
