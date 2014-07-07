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

import gr.ntua.h2rdf.inputFormat2.TableRecordGroupReader;
import gr.ntua.h2rdf.inputFormat2.TableRecordReader2;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;

public class SortMergeJoinReducer extends Reducer<ImmutableBytesWritable, Bindings, Bindings, BytesWritable> {
	public static double samplingRate = 0.05;
	public static int maxSamplePerVariable = 50000;
	public static int partitionBucketKeys = 10000;
	

	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		int sampledBucketKeys = (int)Math.floor(samplingRate * partitionBucketKeys);
		List<TableRecordGroupReader> scanners = new ArrayList<TableRecordGroupReader>();
		int scanPat = context.getConfiguration().getInt("h2rdf.scanPatterns", 0);
		System.out.println("h2rdf.scanPatterns     "+ scanPat);
		int inPat = context.getConfiguration().getInt("h2rdf.inputPatterns", 0);
		System.out.println("h2rdf.inputPatterns     "+ inPat);
		int groups = context.getConfiguration().getInt("h2rdf.inputGroups", 0);
		byte[] table = Bytes.toBytes(context.getConfiguration().get("h2rdf.table", ""));
		int joinVar = context.getConfiguration().getInt("h2rdf.joinVar", 0);

		Map<Integer,TableRecordGroupReader> m = new HashMap<Integer, TableRecordGroupReader>();
		for (int i = 0; i < scanPat; i++) {

			String s1 = context.getConfiguration().get("h2rdf.externalScans_"+i, "");

		    ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(s1));
		    DataInputStream dis = new DataInputStream(bis);
		    Scan scan = new Scan();
		    scan.readFields(dis);
		    

			TableRecordGroupReader val = m.get(Bytes.toInt(scan.getAttribute("group")));
			if(val==null){
				val = new TableRecordGroupReader(Bytes.toString(scan.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME)));
				val.addScan(scan);
				m.put(Bytes.toInt(scan.getAttribute("group")), val);
			}
			else{
				val.addScan(scan);
			}
		    
			/*TableRecordReader2 trr = new TableRecordReader2(scan, table, 0);
			if(trr.nextKeyValue()){
				scanners.add(trr);
			}*/
		}
		for(Entry<Integer, TableRecordGroupReader> e :m.entrySet()){
			if(e.getValue().nextKeyValue()){
				scanners.add(e.getValue());
			}
		}
		

		int countStats=0;
		Map<Byte,Long> stats = new HashMap<Byte, Long>();
		Map<Byte,List<Long>> sampleForPartition = new HashMap<Byte, List<Long>>();
		Random rand = new Random();
		
		boolean jump=false;
		while(jump || context.nextKey()){
			jump=false;
			SortedBytesVLongWritable s = new SortedBytesVLongWritable();
			s.setBytesWithPrefix(context.getCurrentKey().get());
			long k = s.getLong();
			
			if(scanners.size()==groups){
				List<TableRecordGroupReader> nextScanners = new ArrayList<TableRecordGroupReader>();
				Iterator<TableRecordGroupReader> it = scanners.iterator();
				long maxKey=k, firstKey = k;
				//System.out.println();
				//System.out.print(firstKey+" ");
				int i=0;
				while(it.hasNext()){
					TableRecordGroupReader s1 = it.next();
					long key = s1.getJvar();
					//System.out.print(key+" ");
					if(key> maxKey){
						maxKey = key;
					}
					if(key == firstKey){
						i++;
					}
				}
				//System.out.println("maxKey:"+maxKey);
				it = scanners.iterator();
				if((i == groups) && k ==maxKey){//key passed merge join
					int count=0;
					//System.out.println("pased key:"+maxKey);
					HashMap<Integer,List<Bindings>> joinRes = new HashMap<Integer, List<Bindings>>();
					Iterator<Bindings> itv = context.getValues().iterator();
					while(itv.hasNext()){
						/*count++;
						if(count%100==0){
							System.out.println("New key "+count);
						}*/
						Bindings b = itv.next().clone();
						int pat = b.pattern;
						//System.out.println(b.pattern);
						if(joinRes.containsKey(pat)){
							List<Bindings> bin = joinRes.get(pat);
							Bindings.mergeSamePattern(bin, b);
						}
						else{
							List<Bindings> res = new ArrayList<Bindings>();
							res.add(b.clone());
							joinRes.put(pat, res);
						}
						
					}

					if(joinRes.size()==inPat){//passed hash join
						List<Bindings> lres = new ArrayList<Bindings>();
						int first=0, c=0;
						for(Entry<Integer, List<Bindings>> e : joinRes.entrySet()){
							//System.out.println("merge join "+c);
							if(first==0){
								first++;
								lres=e.getValue();
								continue;
							}
							lres = Bindings.merge(lres,e.getValue());
							c++;
							
						}
						
						while(it.hasNext()){
							TableRecordGroupReader s1 = it.next();
							List<Bindings> bk = s1.getCurrentKey();
							//System.out.println(bk.map);
							if(s1.nextKeyValue())
								nextScanners.add(s1);
							else
								s1.close();
							List<Bindings> templ = new ArrayList<Bindings>();
							templ.addAll(bk);
							lres = Bindings.merge(lres,templ);
						}
						
						for(Bindings b : lres){
							b.addBinding((byte)joinVar, k);
							//System.out.println("Output: "+b.map);
							for(Entry<Byte, Set<Long>> e1 : b.map.entrySet()){
								List<Long> samplel=null;
								if(countStats==0){
									stats.put(e1.getKey(), new Long(e1.getValue().size()));
									samplel = new ArrayList<Long>();
									sampleForPartition.put(e1.getKey(), samplel);
								}
								else{
									Long st = stats.get(e1.getKey());
									stats.put(e1.getKey(), new Long(st+e1.getValue().size()));
									samplel = sampleForPartition.get(e1.getKey());
								}
								//partition sampling
								int s1 = b.map.get(e1.getKey()).size();
								double branchingfactor=1.0;
								for(Byte k1 :  b.map.keySet()){
									if(!k1.equals(e1.getKey())){
										branchingfactor = branchingfactor+b.map.get(k1).size();
									}
								}
								double cutoff = samplingRate*branchingfactor;
								if(samplel.size()<maxSamplePerVariable){
									for(Long tl : e1.getValue()){
										if(cutoff>=1){
											for (int j = 0; j < Math.round(cutoff); j++) {
												samplel.add(tl);
												if(samplel.size()>=maxSamplePerVariable){
													break;
												}
											}
										}
										else{
											for (int j = 0; j < branchingfactor; j++) {
												if(rand.nextDouble()<=samplingRate){
													samplel.add(tl);
													if(samplel.size()>=maxSamplePerVariable){
														break;
													}
												}
											}
										}
									}									
								}
								//partition sampling
								
							}
							countStats++;
							//System.out.println(countStats);
							context.write(b, new BytesWritable(new byte[0]));
						}
					}
					
				}
				else{ //move all scanners to maxKey
					while(it.hasNext()){
						TableRecordGroupReader s1 = it.next();
						if(s1.getJvar() < maxKey){
							if(s1.goTo(maxKey))
								nextScanners.add(s1);
							else
								s1.close();
						}
						else{
							nextScanners.add(s1);
						}
					}
					//move context
					long tempk=0;
					if(k < maxKey){
						while(context.nextKey()){
							s = new SortedBytesVLongWritable();
							s.setBytesWithPrefix(context.getCurrentKey().get());
							tempk = s.getLong();
							if(tempk >= maxKey){
								jump=true;
								break;
							}
						}
						if(tempk < maxKey){//end
							return;
						}
					}
					else{
						jump=true;
					}
				}
				
				scanners = nextScanners;
				
			}
			else{
				break;
			}
			
		}

		Counter c = context.getCounter("h2rdf", "sample");
		c.increment(countStats);
		
		for(Entry<Byte, Long> e : stats.entrySet()){
			c = context.getCounter("h2rdf", e.getKey().intValue()+"");
			c.increment(e.getValue());
			//System.out.println(e.getKey().intValue()+" "+e.getValue());
		}

		//find partitions
		String tid = context.getTaskAttemptID().getTaskID().toString();
		tid=tid.substring(tid.lastIndexOf('_'));
		FileSystem fs = FileSystem.get(context.getConfiguration());
		String[] str1 = context.getConfiguration().get("mapred.output.dir").split("/");
		System.out.println("resultPartitions/"+str1[str1.length-1]+"/part"+tid);
		Path path = new Path("resultPartitions/"+str1[str1.length-1]+"/part"+tid);
		if(fs.exists(path))
			fs.delete(path, true);
		FSDataOutputStream out = fs.create(path);
		
		for( Entry<Byte, List<Long>>  e : sampleForPartition.entrySet()){
			List<Long> l = e.getValue();
			Collections.sort(l);
			String part = "";
			
			int i = 0;
			while(i<l.size()){
				part+=l.get(i)+"_";
				i+=sampledBucketKeys;
			}
			if(l.size()>0)
				part+=l.get(l.size()-1)+"";
			System.out.println("h2rdf.partition."+e.getKey().intValue()+" "+part);
			out.writeBytes(e.getKey().intValue()+"$"+part+"\n");
		}
		out.flush();
		out.close();
		
		
		for(TableRecordGroupReader r : scanners){
			r.close();
		}
		
	}


}
