/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.indexScans;
import gr.ntua.h2rdf.inputFormat2.MultiTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.TableMapReduceUtil;
import gr.ntua.h2rdf.inputFormat2.TableRecordGroupReader;
import gr.ntua.h2rdf.inputFormat2.TableRecordReader2;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.loadTriples.TotalOrderPartitioner;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;
import com.hp.hpl.jena.sparql.core.Var;

public class CentralizedMergeJoinExecutor {


	private static FileSystem fs;

	public static List<ResultBGP> execute(MergeJoinPlan plan, Var joinVar, HTable table,
			HTable indexTable) throws IOException, InterruptedException {
		
		
		Configuration conf = new Configuration();
		fs=FileSystem.get(conf);
		Path out =  new Path("output/join_"+JoinExecutor.joinId);
		int countStats=0;
		Map<Integer,double[]> stats = new HashMap<Integer, double[]>();
		/*if(plan.intermediate.size()==0 && plan.scans.size()!=0){//merge join job			
		}
		else if(plan.scans.size()==0 && plan.intermediate.size()!=0){//hash join only
			
		}
		else{ //hybrid do the merge scan in reduce phase*/
			System.out.println("SortMerge Join");
			
			if(fs.exists(out))
				fs.delete(out, true);
			
			int pat=0, count2=0;
			List<Map<Long,List<Bindings>>> lmap = new ArrayList<Map<Long, List<Bindings>>>();
			Byte jVar = OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
			System.out.println("Jvar "+jVar);
			for(ResultBGP e:plan.intermediate){
        		Map<Long,List<Bindings>> map = new TreeMap<Long, List<Bindings>>();
				pat++;
				if(fs.isDirectory(e.path)){
			    	FileStatus[] fss = fs.listStatus(e.path);
			        for (FileStatus status : fss) {
			            Path path = status.getPath();
			            if(path.getName().startsWith("part")){
			            	try{
						    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
						    	Bindings key = new Bindings();
						    	while(reader.next(key)){
						    		//System.out.println(key.map);
							    	Set<Long> val = key.map.get(jVar);
							    	count2++;
									for(Long l : val){
										List<Bindings> v = map.get(l);
										if(v==null){
											/*int found=0;
											for(Map<Long, List<Bindings>> m: lmap){
												if(m.containsKey(l))
													found++;
											}*/
											//if(found==lmap.size()){
												v=new ArrayList<Bindings>();
												v.add(key.clone());
												map.put(l, v);
											//}
										}
										else{
											v.add(key.clone());
										}
									}
								}
						    	reader.close();
			            	}
			            	catch (EOFException e1) {
			            		System.out.println("empty");
							}
			            }
			        }
				}
				else{
					try{
				    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, e.path, conf);
				    	Bindings key = new Bindings();
				    	while(reader.next(key)){
				    		//System.out.println(key.map);
					    	Set<Long> val = key.map.get(jVar);
					    	count2++;
							for(Long l : val){
								List<Bindings> v = map.get(l);
								if(v==null){
									/*int found=0;
									for(Map<Long, List<Bindings>> m: lmap){
										if(m.containsKey(l))
											found++;
									}*/
									//if(found==lmap.size()){
										v=new ArrayList<Bindings>();
										v.add(key.clone());
										map.put(l, v);
									//}
								}
								else{
									v.add(key.clone());
								}
							}
						}
				    	reader.close();
	            	}
	            	catch (EOFException e1) {
	            		System.out.println("empty");
					}
				}
		    	lmap.add(map);
			}
			/*long min = Long.MAX_VALUE;
			for(Map<Long, List<Bindings>> e:lmap){
				Long temp = e.keySet().iterator().next();
				if(temp<min){
					min=temp;
				}
			}*/
			System.out.println("Read keys: "+count2);
			int count =0;
			//if(count2>=10000){

				int g=0;
				List<TableRecordGroupReader> scanners = new ArrayList<TableRecordGroupReader>();
				for(BGP b: plan.scans){
					pat++;
					List<Scan> sc = b.getScans("?"+joinVar.getVarName());
					TableRecordGroupReader groupReader =new TableRecordGroupReader(table);
					for(Scan s : sc){
						s.setCacheBlocks(true); 
						s.setAttribute("stat0", Bytes.toBytes(b.getStatistics(joinVar)[0]));
						s.setAttribute("stat1", Bytes.toBytes(b.getStatistics(joinVar)[1]));
						groupReader.addScan(s);
						
					}
					//TableRecordReader2 trr = new TableRecordReader2(s, table.getTableName(), 0);
					g++;
					if(groupReader.nextKeyValue()){
						scanners.add(groupReader);
					}
					
				}
				System.out.println("InScans: "+g);
				List<Entry<Long,List<Bindings>>> values = new ArrayList<Map.Entry<Long,List<Bindings>>>();
				List<Iterator<Entry<Long, List<Bindings>>>> scanners1 = new ArrayList<Iterator<Entry<Long,List<Bindings>>>>();
				Iterator<Entry<Long, List<Bindings>>> it1;
				for(Map<Long, List<Bindings>> m : lmap){
					it1 = m.entrySet().iterator();
					if(it1.hasNext()){
						scanners1.add(it1);
						values.add(it1.next());
					}
				}
				System.out.println("Scanners: "+scanners.size());

				//System.out.println("list size: "+lmap.size());
				//System.out.println("map size: "+lmap.get(0).size());
				
				SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, out, Bindings.class, BytesWritable.class);
				
				while(scanners.size()+values.size()==pat){
					List<TableRecordGroupReader> nextScanners = new ArrayList<TableRecordGroupReader>();
					List<Entry<Long,List<Bindings>>> nextValues = new ArrayList<Map.Entry<Long,List<Bindings>>>();
					List<Iterator<Entry<Long, List<Bindings>>>> nextScanners1 = new ArrayList<Iterator<Entry<Long,List<Bindings>>>>();
					
					Iterator<TableRecordGroupReader> it = scanners.iterator();
					TableRecordGroupReader sf = it.next();
					long maxKey=sf.getJvar(), firstKey = sf.getJvar();
					//System.out.println();
					//System.out.print(firstKey+" ");
					int i=1;
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
					
					for(Entry<Long,List<Bindings>> e :values){
						long key = e.getKey();
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
					if(i == pat){//key passed
						//System.out.println("Key passed");
						Bindings b = new Bindings();

						List<Bindings> lres = new ArrayList<Bindings>();
						int first=0;
						
						while(it.hasNext()){
							TableRecordGroupReader s1 = it.next();
							Bindings bk = s1.getCurrentKey();
							if(s1.nextKeyValue())
								nextScanners.add(s1);
							else
								s1.close();
							
							List<Bindings> templ = new ArrayList<Bindings>();
							templ.add(bk);
							if(first==0){
								first++;
								lres=templ;
								continue;
							}
							lres = Bindings.merge(lres,templ);
							//System.out.println(bk.map);
							//b.addAll(bk);
						}
						for(Entry<Long,List<Bindings>> e :values){
							lres = Bindings.merge(lres, e.getValue());
						}
						int k =0;
						for(Iterator<Entry<Long, List<Bindings>>> it2 : scanners1){
							if(it2.hasNext()){
								nextValues.add(it2.next());
								nextScanners1.add(it2);
							}
							k++;
						}
						
						for(Bindings b1 : lres){
							for(Entry<Byte, Set<Long>> e1 : b1.map.entrySet()){
								if(countStats==0){
									double[] st = new double[2];
									st[0]=e1.getValue().size();
									st[1]=1;
									stats.put(e1.getKey().intValue(), st);
								}
								else{
									double[] st = stats.get(e1.getKey().intValue());
									st[1]+=e1.getValue().size();
								}
							}
							countStats++;
							count++;
							//b1.print(indexTable);
							//System.out.println("Output: "+b1.map);
							writer.append(b1, new BytesWritable(new byte[0]));
						}
						//System.out.println(b.map);
						//context.write(b, new BytesWritable());
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
						int k=0;
						for(Iterator<Entry<Long, List<Bindings>>> it2 : scanners1){
							if(values.get(k).getKey() < maxKey){
								//System.out.println("Moving iterator: "+k+" value: "+values.get(k).getKey());
								while(it2.hasNext()){
									Entry<Long, List<Bindings>> d = it2.next();
									//System.out.println("next: "+d.getKey());
									if(d.getKey()>=maxKey){
										nextValues.add(d);
										nextScanners1.add(it2);
										break;
									}
								}
							}
							else{
								nextValues.add(values.get(k));
								nextScanners1.add(it2);
							}
							k++;
						}
						
					}
					scanners = nextScanners;
					values = nextValues;
					scanners1 = nextScanners1;
					//if(count%100==0)
					//	System.out.println(count);
				}
				System.out.println("Results: "+count);
				writer.close();
				
			//}
			//else{
				
			//}
			
		//}
		if(count==0){
			OptimizeOpVisitorMergeJoin.finished=true;
		}
		return resultPlan(plan,out,stats);
	}
	
	private static List<ResultBGP> resultPlan(MergeJoinPlan plan, Path out, Map<Integer, double[]> stats) throws IOException {
		List<ResultBGP> ret = new ArrayList<ResultBGP>();
    	Set<Var> vars = new HashSet<Var>();
		for(BGP e:plan.scans){
			vars.addAll(e.joinVars);
		}
		for(ResultBGP e:plan.intermediate){
			vars.addAll(e.joinVars);
		}
		long sum =0;
		/*if(fs.isDirectory(out)){
	    	FileStatus[] fss = fs.listStatus(out);
	        for (FileStatus status : fss) {
	            Path path = status.getPath();
	            if(path.getName().startsWith("part"))
	            	sum+=fs.getContentSummary(path).getLength()/vars.size()/4;
	        }
    	}
		else{
            sum+=fs.getContentSummary(out).getLength()/vars.size()/4;
		}
		System.out.println("Output size: "+sum);*/
    	ret.add(new ResultBGP(vars, out, stats));
		return ret;
	}

}
