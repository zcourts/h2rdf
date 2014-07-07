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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;


public class Solve2 {
	public static TreeMap<Integer, List<String>> g = new TreeMap<Integer, List<String>>();
	public static HashMap<BitSet,String>dptable =new HashMap<BitSet, String>();
	public static HashMap<String,BitSet>varIndex =new HashMap<String,BitSet>();
	public static HashMap<Integer,String> index = new HashMap<Integer, String>();
	public static int n, totalChecks, cacheChecks;
	
	public static void main(String[] args) {
		//setup();
		ResultCache cache = new ResultCache();
		cache.initialize();
		index.put(1,"takesCourse");
		index.put(2,"teacher");
		index.put(3,"advisor");
		index.put(4,"typeCourse");
		index.put(5,"typeProf");
		index.put(6,"typeStudent");
		n=6;
		totalChecks=0;
		cacheChecks=0;
		
		List<String> l = new ArrayList<String>(); 
		l.add("?x 6");
		l.add("?x 3");
		l.add("?y 4");
		l.add("?y 2");
		g.put(1, l);
		
		l = new ArrayList<String>(); 
		l.add("?x 6");
		l.add("?x 1");
		l.add("?z 5");
		l.add("?z 2");
		g.put(3, l);

		l = new ArrayList<String>(); 
		l.add("?z 5");
		l.add("?z 3");
		l.add("?y 4");
		l.add("?y 1");
		g.put(2, l);
		
		l = new ArrayList<String>(); 
		l.add("?x 3");
		l.add("?x 1");
		g.put(6, l);
		
		l = new ArrayList<String>(); 
		l.add("?y 2");
		l.add("?y 1");
		g.put(4, l);
		
		l = new ArrayList<String>(); 
		l.add("?z 2");
		l.add("?z 3");
		g.put(5, l);
		
		BitSet x = new BitSet(n);
		x.set(1);
		x.set(3);
		x.set(6);
		varIndex.put("?x", x);
		
		x = new BitSet(n);
		x.set(1);
		x.set(4);
		x.set(2);
		varIndex.put("?y", x);
		
		x = new BitSet(n);
		x.set(2);
		x.set(3);
		x.set(5);
		varIndex.put("?z", x);
		
		solve();		
	}

	private static void setup() {

		index.put(1,"p1");
		index.put(2,"p2");
		index.put(3,"p3");
		index.put(4,"p4");
		index.put(5,"p5");
		index.put(6,"p6");
		index.put(7,"p7");
		index.put(8,"p8");
		n=8;
		totalChecks=0;
		cacheChecks=0;
		
		List<String> l = new ArrayList<String>(); 
		l.add("?y 2");
		l.add("?z 6");
		g.put(1, l);
		
		l = new ArrayList<String>(); 
		l.add("?y 1");
		l.add("?x 3");
		l.add("?x 4");
		l.add("?x 7");
		g.put(2, l);

		l = new ArrayList<String>(); 
		l.add("?x 2");
		l.add("?x 4");
		l.add("?x 7");
		l.add("?w 8");
		g.put(3, l);
		
		l = new ArrayList<String>(); 
		l.add("?x 2");
		l.add("?x 3");
		l.add("?x 7");
		l.add("?n 5");
		g.put(4, l);
		
		l = new ArrayList<String>(); 
		l.add("?n 4");
		g.put(5, l);
		
		l = new ArrayList<String>(); 
		l.add("?z 1");
		g.put(6, l);
		
		l = new ArrayList<String>(); 
		l.add("?x 2");
		l.add("?x 3");
		l.add("?x 4");
		l.add("?l 8");
		g.put(7, l);
		
		l = new ArrayList<String>(); 
		l.add("?w 3");
		l.add("?l 7");
		g.put(8, l);
		
		
		BitSet x = new BitSet(n);
		x.set(2);
		x.set(3);
		x.set(4);
		x.set(7);
		varIndex.put("?x", x);
		
		x = new BitSet(n);
		x.set(1);
		x.set(2);
		varIndex.put("?y", x);
		
		x = new BitSet(n);
		x.set(1);
		x.set(6);
		varIndex.put("?z", x);
		
		x = new BitSet(n);
		x.set(7);
		x.set(8);
		varIndex.put("?l", x);
		
		x = new BitSet(n);
		x.set(3);
		x.set(8);
		varIndex.put("?w", x);
		
		x = new BitSet(n);
		x.set(4);
		x.set(5);
		varIndex.put("?n", x);
	}

	private static void solve() {
		for(Integer v : g.descendingKeySet()){
			BitSet b = new BitSet(n);
			b.set(v);
			System.out.println(b+ " scan "+index.get(v));
			dptable.put(b, "Scan "+index.get(v));
		}
		
		for(Integer v : g.descendingKeySet()){
			BitSet b = new BitSet(n);
			b.set(v);
			emitCsg(b);
			BitSet bv = new BitSet(n);
			for (int i = 1; i <= v; i++) {
				bv.set(i);
			}
			enumerateCsgRec(b,bv);
		}
		BitSet b = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			b.set(i);
		}
		System.out.println("Cache Checks: "+cacheChecks);
		System.out.println("Join Checks: "+totalChecks);
		System.out.println("Optimal: "+ dptable.get(b));
	}

	private static void enumerateCsgRec(BitSet b, BitSet bv) {
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



	private static void emitCsg(BitSet s1) {
		System.out.println("Check cache s:"+s1);
		//check cache!!!!!
		cacheChecks++;
		//if(s1.get(3)&&s1.get(5)&&!s1.get(2)&&!s1.get(1))
		//	dptable.put(s1, "{"+s1+" from cache }");
		//if(cacheChecks%20==0)
			//dptable.put(s1, " ");
		
		//System.out.println("EmitCsg S1: "+s1);
		BitSet X = new BitSet(n);
		int mins1=s1.nextSetBit(0);
		for (int i = 1; i <= mins1; i++) {
			X.set(i);
		}
		X.or(s1);
		
		//System.out.println("X: "+ X);
		HashMap<String, BitSet> map = neighbor2(s1,X);
		//System.out.println("Neighbor: "+map);
		//multiway join enumeration
		for(String var :map.keySet()){
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
			for (int i = 1; i <=N.size() ; i++) {
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
			
		}
		
	}
	
	private static void enumerateCsgList(List<BitSet> l, int index, BitSet X, String var) {
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

	private static void emitCsgList(List<BitSet> l, String var) {
		System.out.println("Emit multi-way join "+var+": "+l);
		totalChecks++;
		BitSet s = new BitSet(n);
		for(BitSet b : l){
			if(!dptable.containsKey(b))
				return;
			s.or(b);
		}
		if(!dptable.containsKey(s)){
			String st ="{join "+var+": ";
			for(BitSet b : l){
				st+=dptable.get(b)+" ";
			}
			st+="}";
			dptable.put(s, st);
			System.out.println("Insert "+s+"  "+st);
		}
			//System.out.println("Check cache s:"+s);
	}


	public static BitSet neighbor(BitSet S, BitSet X) {
		BitSet N = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			if(S.get(i)){
				for(String s : g.get(i)){
					N.set(Integer.parseInt(s.substring(3)));
				}
			}
		}
		N.andNot(X);
		return N;
	}

	private static HashMap<String, BitSet> neighbor2(BitSet S, BitSet X) {
		HashMap<String,BitSet> map = new HashMap<String, BitSet>();
		HashMap<String,BitSet> map1 = new HashMap<String, BitSet>();
		for (int i = 1; i <= n; i++) {
			if(S.get(i)){
				for(String s : g.get(i)){
					String[] k=s.split(" ");
					if(!X.get(Integer.parseInt(k[1]))){
						BitSet entry = map.get(k[0]);
						if(entry==null){
							entry = new BitSet(n);
							entry.set(Integer.parseInt(k[1]));
							map.put(k[0], entry);
							entry = new BitSet(n);
							entry.set(i);
							map1.put(k[0], entry);
						}
						else{
							entry.set(Integer.parseInt(k[1]));
							entry =map1.get(k[0]);
							entry.set(i);
							
						}
					}
				}
			}
		}
		
		
		HashMap<String,BitSet> ret = new HashMap<String, BitSet>();
		for(String var : map.keySet()){
			map1.get(var).or(map.get(var));
			if(varIndex.get(var).equals(map1.get(var))){
				ret.put(var, map.get(var));
			}
		}
		return ret;
	}

}
