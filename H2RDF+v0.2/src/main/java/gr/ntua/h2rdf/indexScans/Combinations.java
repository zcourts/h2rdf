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
package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

public class Combinations {
	private List<Set<Long>> m ;
	private List<Iterator<Long>> it;
	private int currentLevel;
	private List<Long> currentValue;
	
	public Combinations(Bindings kb, List<Integer> orderVarsInt) {
		this.m =new ArrayList<Set<Long>>();
		it = new ArrayList<Iterator<Long>>();
		for(Integer i : orderVarsInt){
			Set<Long> v = kb.map.get((byte)(int)i);
			m.add(v);
			it.add(v.iterator());
		}
		currentValue = new ArrayList<Long>();
		currentLevel=m.size()-1;
	}

	public Combinations(Bindings kb, ArrayList<Byte> joinVars) {
		this.m =new ArrayList<Set<Long>>();
		it = new ArrayList<Iterator<Long>>();
		for(Byte b : joinVars){
			Set<Long> v = kb.map.get(b);
			m.add(v);
			it.add(v.iterator());
		}
		currentValue = new ArrayList<Long>();
		currentLevel=m.size()-1;
	}

	public List<Long> nextBinding() throws IOException {
		//System.out.println("Level: "+currentLevel);
		//System.out.println(currentValue);
		List<Long> out = new ArrayList<Long>();
		int i = 0;
		for(Iterator<Long> iter : it){
			if(i < currentLevel){
				if(currentValue.size()<m.size()){//first time
					if(!iter.hasNext())
						return null;
					currentValue.add(i,iter.next());
				}
				out.add(currentValue.get(i));
			}
			else{
				if(iter.hasNext()){
					Long l = iter.next();
					out.add(l);
					if(currentValue.size()==m.size()){
						currentValue.remove(i);
					}
					currentValue.add(i, l);
				}
				else{//restart
					if(i==0){//finished
						return null;
					}
					else if(i==currentLevel){
						currentLevel--;
						it.remove(i);
						it.add(i, m.get(i).iterator());
						return nextBinding();
					}
					/*else{
						iter = m.get(i).iterator();
						if(iter.hasNext()){
							Long l = iter.next();
							ret+=l+"_";
							currentValue.add(i, l);
						}
					}*/
				}
			}
			i++;
		}
		currentLevel=m.size()-1;
		if(out.isEmpty())
			return null;
		return out;
		
	}
	
	public byte[] next() throws IOException {
		//System.out.println("Level: "+currentLevel);
		//System.out.println(currentValue);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int i = 0;
		for(Iterator<Long> iter : it){
			if(i < currentLevel){
				if(currentValue.size()<m.size()){//first time
					if(!iter.hasNext())
						return null;
					currentValue.add(i,iter.next());
				}
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(currentValue.get(i));
				out.write(v.getBytesWithPrefix());
			}
			else{
				if(iter.hasNext()){
					Long l = iter.next();
					SortedBytesVLongWritable v = new SortedBytesVLongWritable(l);
					out.write(v.getBytesWithPrefix());
					if(currentValue.size()==m.size()){
						currentValue.remove(i);
					}
					currentValue.add(i, l);
				}
				else{//restart
					if(i==0){//finished
						return null;
					}
					else if(i==currentLevel){
						currentLevel--;
						it.remove(i);
						it.add(i, m.get(i).iterator());
						return next();
					}
					/*else{
						iter = m.get(i).iterator();
						if(iter.hasNext()){
							Long l = iter.next();
							ret+=l+"_";
							currentValue.add(i, l);
						}
					}*/
				}
			}
			i++;
		}
		currentLevel=m.size()-1;
		out.flush();
		return out.toByteArray();
	}


	public static void main(String[] args) {
		Bindings b = new Bindings();
		
		Set<Long> l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)1, l);
		
		l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)2, l);
		
		l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)3, l);
		
		l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)4, l);
		
		List<Integer> order = new ArrayList<Integer>();
		order.add(1);
		order.add(2);
		order.add(3);
		order.add(4);
		Combinations c = new Combinations(b, order);
		byte[] s;
		try {
			while((s = c.next())!=null){
				System.out.println(Bytes.toStringBinary(s));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
