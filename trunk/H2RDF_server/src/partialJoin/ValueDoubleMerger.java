/*******************************************************************************
 * Copyright (c) 2012 Nikos Papailiou. 
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Nikos Papailiou - initial API and implementation
 ******************************************************************************/
package partialJoin;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.sail.rdbms.managers.HashManager;

public class ValueDoubleMerger {
	

	private HashMap<String, String>[] bindings = null;
	private HashMap<String, HashSet<String>>[] pat_no= null;
	private HashSet<String> patterns= null;
	private String totValue;
	private String[] doubleVars;
	private Integer[] doubleVarsNum;
	
	private Iterator<String> e;
	private int numOfDoubleVars;

	public ValueDoubleMerger(Configuration conf) {
		totValue="";
		patterns= new HashSet<String>();
		numOfDoubleVars= conf.getInt("input.double",0);
		if(numOfDoubleVars>0){
			doubleVarsNum = new Integer[numOfDoubleVars];
			doubleVars = new String[numOfDoubleVars];
			bindings= new HashMap[numOfDoubleVars];
			pat_no= new HashMap[numOfDoubleVars];
			for (int i = 0; i < numOfDoubleVars; i++) {
				int c =i+1;
				doubleVars[i] = conf.get("input.double"+c);
				doubleVarsNum[i] = conf.getInt("input.double"+c+".num",2);
				bindings[i]=new HashMap<String, String>();
				pat_no[i]=new HashMap<String, HashSet<String>>();
			}
		}
	}
	
	public void merge(String v, String pat) {

		patterns.add(pat);
		if(numOfDoubleVars>0){
			
			StringTokenizer t = new StringTokenizer(v);
			List<String>[] b = new List[numOfDoubleVars];
			for (int i = 0; i < b.length; i++) {
				b[i] = new ArrayList<String>();
			}
			String else_val="";
			boolean foundOne= false;
			int varId=0;
			while(t.hasMoreTokens()){
				String binding = t.nextToken("!");
				StringTokenizer tok = new StringTokenizer(binding);
				String var = tok.nextToken("#");
				boolean found=false;
				for (int i = 0; i < numOfDoubleVars; i++) {//den leitourgei gia x y z |x y z
					if(doubleVars[i].equals(var)){
						varId=i;
						found=true;
						break;
					}
				}
				
				if(found){
					b[varId].add(tok.nextToken());
					foundOne=true;
				}
				else{
					else_val+=binding+"!";
				}
			}
			if(foundOne){
				for (int i = 0; i < numOfDoubleVars; i++) {
					ListIterator<String> it = b[i].listIterator();
					while(it.hasNext()){
						collect(it.next(), pat, else_val, varId);
					}
				}
			}
			else{
				totValue+=v;
			}
		}
		else{
			totValue+=v;
		}
	}

	private void collect(String binding, String pat, String else_val, int varId) {
		String tpat, value, el;
		
		StringTokenizer bintok = new StringTokenizer(binding);
		while(bintok.hasMoreTokens()){
			String bin = bintok.nextToken("_");
			
			if(bindings[varId].containsKey(bin)){
				value = bindings[varId].get(bin);
				value+=else_val;
				bindings[varId].put(bin, value);
				
				HashSet<String> pat1 = pat_no[varId].get(bin);
				pat1.add(pat);
				pat_no[varId].put(bin, pat1);
			}
			else{
				bindings[varId].put(bin, else_val);
				HashSet<String> pat1 = new HashSet<String>();
				pat1.add(pat);
				pat_no[varId].put(bin, pat1);
				//bindings.put(bin, pat+"$"+else_val);
			}
		}
		
		
		
	}

	public String getValue() {
		String key, tpat, el, value, out="";
		key = e.next();
		value = bindings[0].get(key);
		HashSet<String> pats = pat_no[0].get(key);
		if(pats.size()==doubleVarsNum[0]){
			out+=doubleVars[0]+"#"+key+"_!"+value;
		}
		
		return out;
	}

	public boolean hasMore() {
		return e.hasNext();
	}

	public boolean itter() {
		if(bindings!= null){
			e = bindings[0].keySet().iterator();
			return true;
		}
		else{
			return false;
		}
	}

	public int getTotalPatterns() {
		return patterns.size();
	}

	public String getTotal() {
		return totValue;
	}

}
