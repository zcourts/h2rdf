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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class JoinBGPReducer extends Reducer<ImmutableBytesWritable, Bindings, Bindings, BytesWritable> {

	private MultipleOutputs mos;
	private HashMap<Integer,Integer> numVars;
	private int count;
	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Bindings> values,Context context)
			throws IOException, InterruptedException {
		int joinVar = key.get()[0];
		if(!numVars.containsKey(joinVar)){
			//System.out.println("h2rdf.inputPatterns_"+joinVar+"   "+context.getConfiguration().getInt("h2rdf.inputPatterns_"+joinVar, 0));
			numVars.put(joinVar, context.getConfiguration().getInt("h2rdf.inputPatterns_"+joinVar, 0));
		}
		int num = numVars.get(joinVar);
		//System.out.println(joinVar+" patterns: "+num);
		HashMap<Integer,List<Bindings>> joinRes = new HashMap<Integer, List<Bindings>>();
		
		ByteArrayInputStream in = new ByteArrayInputStream(key.get());
		byte[] b1 =new byte[1];
		in.read(b1, 0, 1);
		Set<Long> s = new HashSet<Long>();
		s.add( SortedBytesVLongWritable.readLong(in));
		
		Iterator<Bindings> it = values.iterator();
		while(it.hasNext()){
			Bindings b = it.next().clone();
			int pat = b.pattern;
			//System.out.println(b.pattern+" "+b.map);
			if(joinRes.containsKey(pat)){
				List<Bindings> bin = joinRes.get(pat);
				bin.add(b);
			}
			else{
				List<Bindings> res = new ArrayList<Bindings>();
				res.add(b);
				joinRes.put(pat, res);
			}

			
		}
		
		/*System.out.println("Printing map:");
		for(Entry<Integer, List<Bindings>> e : joinRes.entrySet()){
			System.out.println("Pat: "+e.getKey()+" ");
			for(Bindings b : e.getValue()){
				System.out.println(b.map);
			}
			
		}*/
		
		
		//System.out.println("Found patterns: "+joinRes.size());
		if(joinRes.size()==num){
			List<Bindings> lres = new ArrayList<Bindings>();
			int first=0;
			for(Entry<Integer, List<Bindings>> e : joinRes.entrySet()){
				if(first==0){
					first++;
					lres=e.getValue();
					continue;
				}
				lres = Bindings.merge(lres,e.getValue());
				
			}
			for(Bindings b : lres){
				b.map.put((byte)joinVar, s);
				//System.out.println("Output: "+joinVar+" "+b.map);
				//if(b.map.size()==num)
				mos.write(joinVar+"", b, new BytesWritable(new byte[0]));
				count++;
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
		System.out.println(count);
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs(context);
		numVars = new HashMap<Integer, Integer>();
		count = 0;
	}

}
