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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

public class OrderingReducer extends Reducer<ImmutableBytesWritable, Bindings, ImmutableBytesWritable, KeyValue> {

	private HashMap<Byte, Long> stats;
	private ArrayList<Byte> joinVars;

	public void reduce(ImmutableBytesWritable key, Iterable<Bindings> values, Context context) throws IOException {
		
		for(Byte joinVar : joinVars){
			Long st1 = stats.get(joinVar);
			if(st1==null){
				stats.put(joinVar, new Long(1));
			}
			else{
				stats.put(joinVar, new Long(st1+1));
			}
		}
		
		short q=0;
		//System.out.println("New key: "+s.getLong());
		ByteArrayOutputStream outStream = new ByteArrayOutputStream(); 
		DataOutputStream out = new DataOutputStream(outStream);
		HashMap<Integer,List<Bindings>> joinRes = new HashMap<Integer, List<Bindings>>();
		for(Bindings b : values){
			int pat = b.pattern;
			if(joinRes.containsKey(pat)){
				List<Bindings> bin = joinRes.get(pat);
				Bindings.mergeSamePattern(bin, b);
			}
			else{
				List<Bindings> res = new ArrayList<Bindings>();
				res.add(b.clone());
				joinRes.put(pat, res);
			}
			//sum.addAll(b);
			//b.write(out);
			/*if(out.size()>=64000){
				out.flush();
				KeyValue emmitedValue = new KeyValue(key.get().clone(), Bytes.toBytes("I"), Bytes.toBytes(q), outStream.toByteArray().clone());
				try {
			    	context.write(new ImmutableBytesWritable(key.get().clone()), emmitedValue);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				outStream = new ByteArrayOutputStream(); 
				out = new DataOutputStream(outStream);
				q++;
				//System.out.println(q);
			}*/
		}
		if(joinRes.size()>=0){
			List<Bindings> lres = new ArrayList<Bindings>();
			int first=0;
			for(Entry<Integer, List<Bindings>> e : joinRes.entrySet()){
				//System.out.println("merge join "+c);
				if(first==0){
					first++;
					lres=e.getValue();
					continue;
				}
				lres = Bindings.merge(lres,e.getValue());
			}
			//System.out.println(lres.size());
			for(Bindings b : lres){
				for(Entry<Byte, Set<Long>> e1 : b.map.entrySet()){
					Long st = stats.get(e1.getKey());
					if(st==null){
						stats.put(e1.getKey(), new Long(e1.getValue().size()));
					}
					else{
						stats.put(e1.getKey(), new Long(st+e1.getValue().size()));
						
					}
				}
				b.write(out);
			}
			out.flush();
			KeyValue emmitedValue = new KeyValue(key.get().clone(), Bytes.toBytes("I"), Bytes.toBytes(q), outStream.toByteArray().clone());
			try {
		    	context.write(new ImmutableBytesWritable(key.get().clone()), emmitedValue);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			outStream = new ByteArrayOutputStream(); 
			out = new DataOutputStream(outStream);
			//System.out.println(q);
		}
		
	}

	@Override
	protected void cleanup(Context context) throws IOException ,InterruptedException {
		for(Entry<Byte, Long> e : stats.entrySet()){
			Counter c = context.getCounter("h2rdf", e.getKey().intValue()+"");
			c.increment(e.getValue());
			//System.out.println(e.getKey().intValue()+" "+e.getValue());
		}
	}

	@Override
	protected void setup(Context context) throws IOException ,InterruptedException {
		super.setup(context);
		String s = context.getConfiguration().get("h2rdf.joinVar");
		String s1[] = s.split("_");
		joinVars = new ArrayList<Byte>();
		for (int i = 0; i < s1.length; i++) {
			joinVars.add(Byte.parseByte(s1[i]));
		}
		System.out.println("OrderVars: "+joinVars);
		stats = new HashMap<Byte, Long>();
	}
	
	
}
