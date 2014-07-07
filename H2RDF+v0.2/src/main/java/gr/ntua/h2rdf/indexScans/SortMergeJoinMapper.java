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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SortMergeJoinMapper extends Mapper<BytesWritable, BytesWritable, ImmutableBytesWritable, Bindings> {
	
	private boolean table;
	private boolean file;
	private boolean hasRelabeling;
	private Scan scan;
	private byte pattern;
	private HashMap<Integer, Integer> varRelabeling;//key:file varId, value newqueryVarId
	private boolean hasSelective;
	private HashMap<Integer, Long> selectiveBindings;
	private ArrayList<Byte> joinVars;

	public void map(BytesWritable key, BytesWritable value, Context context) throws IOException {
		/*int vars= ran.nextInt(100);
		Bindings b = new Bindings();
		for (int i = 0; i < vars; i++) {
			int s= ran.nextInt(100000);
			for (int j = 0; j < s; j++) {

				b.addBinding((byte)i, ran.nextLong());
			}
			System.out.println(b.map.size());
		}*/
		try {
		//	Bindings b = (Bindings)key;
			Bindings b = null;
			if(key.getClass().equals(Bindings.class)){
				b=(Bindings)key;
			}
			else{
				b = new Bindings();
				if(hasRelabeling||hasSelective)
					b.readFields(key.getBytes(), varRelabeling,selectiveBindings);
				else
					b.readFields(key.getBytes());
			}
			
			if(!b.valid)
				return;
			//if(table){
			//System.out.println(b.map);

    		Bindings kb = new Bindings();
    		for(Byte i : joinVars){
    			kb.map.put(i, b.map.remove(i));
    		}
    		Combinations c = new Combinations(kb, joinVars);
    		byte[] s;
    		Map<byte[],List<Bindings>> map = new TreeMap<byte[], List<Bindings>>(Bytes.BYTES_COMPARATOR);
    		while((s = c.next())!=null){
				List<Bindings> v = map.get(s);
				if(v==null){
					v=new ArrayList<Bindings>();
					Bindings tk = b.clone();
					v.add(tk);
					map.put(s, v);
				}
				else{
					Bindings.mergeSamePattern(v, b.clone());
				}
    		}
    		for(Entry<byte[], List<Bindings>> e : map.entrySet()){

				ImmutableBytesWritable i = new ImmutableBytesWritable(e.getKey());
				for(Bindings b1 : e.getValue()){
					b1.pattern=pattern;
					context.write(i, b1);
					//SortedBytesVLongWritable sv = new SortedBytesVLongWritable();
					//sv.setBytesWithPrefix(e.getKey());
					
					//System.out.println("Key: "+ sv.getLong()+" value: "+b1.map);
				}
    		}
			/*Set<Long> val = b.map.remove(joinVar);
				
			for(Long l : val){
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(l);
				byte[] k1 = v.getBytesWithPrefix();
				ImmutableBytesWritable i = new ImmutableBytesWritable(k1);
				b.pattern=pattern;
				
				context.write(i, b);
				
			}*/
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		String name = ((FileSplit)context.getInputSplit()).getPath().toUri().toString();
		System.out.println(name);
		name = name.split("/part")[0];
	    System.out.println("h2rdf.inputFiles_"+name+"  "+ context.getConfiguration().getInt("h2rdf.inputFiles_"+name, 0));
	    System.out.println("h2rdf.inputFilesVar_"+name+"  "+ context.getConfiguration().getInt("h2rdf.inputFilesVar_"+name, 0));
		pattern = (byte)context.getConfiguration().getInt("h2rdf.inputFiles_"+name, 0);
		String s = context.getConfiguration().get("h2rdf.inputFilesVar_"+name);
		String s1[] = s.split("_");
		joinVars = new ArrayList<Byte>();
		for (int i = 0; i < s1.length; i++) {
			joinVars.add(Byte.parseByte(s1[i]));
		}
		//joinVar = (byte)context.getConfiguration().getInt("h2rdf.inputFilesVar_"+name, 0);
		hasRelabeling = context.getConfiguration().getBoolean("h2rdf.inputFilesHasRelabeling_"+name, false);
		if(hasRelabeling){
			varRelabeling= new HashMap<Integer, Integer>();
			int size = context.getConfiguration().getInt("h2rdf.inputFilesRelabelingSize_"+name, 0);
			for (int i = 0; i < size; i++) {
				Integer value = context.getConfiguration().getInt("h2rdf.inputFilesRelabeling_"+name+"_"+i, 0);
				varRelabeling.put(i, value);
			}
			System.out.println("varRelabeling: "+varRelabeling);
		}
		hasSelective = context.getConfiguration().getBoolean("h2rdf.inputFilesHasSelective_"+name, false);
		if(hasSelective){
			selectiveBindings= new HashMap<Integer, Long>();
			int size = context.getConfiguration().getInt("h2rdf.inputFilesSelectiveSize_"+name, 0);
			for (int i = 0; i < size; i++) {
				String value = context.getConfiguration().get("h2rdf.inputFilesSelective_"+name+"_"+i, "");
				String[] v = value.split("_");
				selectiveBindings.put(Integer.parseInt(v[0]), Long.parseLong(v[1]));
			}
			System.out.println("selectiveBindings: "+selectiveBindings);
		}
		System.out.println((int)pattern+" "+joinVars);
	}
	
	
}
