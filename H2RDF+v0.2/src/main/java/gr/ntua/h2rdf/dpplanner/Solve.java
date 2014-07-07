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


public class Solve {
	public static TreeMap<Integer, List<String>> g = new TreeMap<Integer, List<String>>();
	public static HashMap<BitSet,String>dptable =new HashMap<BitSet, String>();
	public static HashMap<Integer,String> index = new HashMap<Integer, String>();

	public static int n, totalChecks;
	public static void main(String[] args) {
		index.put(1,"takesCourse");
		index.put(2,"teacher");
		index.put(3,"advisor");
		index.put(4,"typeCourse");
		index.put(5,"typeProf");
		index.put(6,"typeStudent");

		totalChecks=0;
		
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
		
		solve();
		

		System.out.println("Checked: "+totalChecks);
	}

	private static void solve() {
		for(Integer v : g.descendingKeySet()){
			BitSet b = new BitSet(6);
			b.set(v);
			System.out.println(b+ " scan "+index.get(v));
			dptable.put(b, "Scan "+index.get(v));
		}
		
		for(Integer v : g.descendingKeySet()){
			BitSet b = new BitSet(6);
			b.set(v);
			emitCsg(b);
			BitSet bv = new BitSet(6);
			for (int i = 1; i <= v; i++) {
				bv.set(i);
			}
			enumerateCsgRec(b,bv);
		}
		BitSet b = new BitSet(6);
		for (int i = 1; i <= 6; i++) {
			b.set(i);
		}
		//System.out.println("Optimal: "+ dptable.get(b));
	}

	private static void enumerateCsgRec(BitSet b, BitSet bv) {
		//System.out.println("EnumerateCsgRec S1: "+b+" X: "+bv);
		BitSet N = neighboor(b,bv);
		//System.out.println("N: "+N);
		if(N.isEmpty())
			return;
		PowerSet pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(b);
        		//System.out.println("Check DPtable: "+t);
        		if(dptable.containsKey(t)){
            		emitCsg(t);
        		}
        	}
        }
		
        pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(b);
        		BitSet Xnew = new BitSet(6);
        		Xnew.or(bv);
        		Xnew.or(N);
        		enumerateCsgRec(t,Xnew);
        	}
        }
		
		
		
	}



	private static void emitCsg(BitSet s1) {
		//System.out.println("EmitCsg S1: "+s1);
		BitSet X = new BitSet(6);
		int mins1=s1.nextSetBit(0);
		for (int i = 1; i <= mins1; i++) {
			X.set(i);
		}
		X.or(s1);
		//System.out.println("X: "+ X);
		BitSet N = neighboor(s1,X);
		//System.out.println("N: "+N);
		for (int i = N.size(); i >=1 ; i--) {
			BitSet s2 = new BitSet(6);
			if(N.get(i)){
        		s2.set(i);
            	//removed check for connectedness
        		emitCsgCmp(s1,s2);
        		enumerateCmpRec(s1,s2,X);
			}
		}
	}
	
	private static void enumerateCmpRec(BitSet s1, BitSet s2, BitSet X) {
		BitSet N = neighboor(s2,X);
		if(N.isEmpty())
			return;
		PowerSet pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(s2);
        		//System.out.println("Check DPtable: "+t);
        		if(dptable.containsKey(t)){
            		emitCsgCmp(s1,t);
        		}
        	}
        }
        
        X.or(N);
        N = neighboor(s2,X);
		if(N.isEmpty())
			return;
		pset = new PowerSet(N);
        for(BitSet t:pset)
        {
        	if(!t.isEmpty()){
        		t.or(s2);
        		enumerateCmpRec(s1,t,X);
        	}
        }
	}

	private static void emitCsgCmp(BitSet s1, BitSet s2) {
		System.out.println("EmitCsgCmp s1:"+s1+" s2: "+s2);
		totalChecks++;
		String pl1 = dptable.get(s1);
		String pl2 = dptable.get(s2);
		BitSet s = new BitSet(6);
		s.or(s1);
		s.or(s2);
		//if(!dptable.containsKey(s))
			//System.out.println("Check cache s:"+s);
		dptable.put(s, pl1+" "+pl2);
	}

	public static BitSet neighboor(BitSet S, BitSet X) {
		BitSet N = new BitSet(6);
		for (int i = 1; i <= 6; i++) {
			if(S.get(i)){
				for(String s : g.get(i)){
					N.set(Integer.parseInt(s.substring(3)));
				}
			}
		}
		N.andNot(X);
		return N;
	}
}
