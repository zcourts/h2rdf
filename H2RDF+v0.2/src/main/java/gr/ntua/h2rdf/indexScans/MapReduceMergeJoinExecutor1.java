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

import gr.ntua.h2rdf.dpplanner.CachedResult;
import gr.ntua.h2rdf.dpplanner.IndexScan;
import gr.ntua.h2rdf.inputFormat2.MultiTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.TableMapReduceUtil;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.loadTriples.TotalOrderPartitioner;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class MapReduceMergeJoinExecutor1 {
	private static FileSystem fs;

	public static List<ResultBGP> execute(MergeJoinPlan plan, Var joinVar, HTable table,
			HTable indexTable, OptimizeOpVisitorDPCaching visitor) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		fs=FileSystem.get(conf);
		Job job=null;
		String outname = "join_"+visitor.cachingExecutor.id+"_"+visitor.cachingExecutor.tid;
		Path out =  new Path("output/"+outname);
		Path p = new Path("resultPartitions/"+outname);
		if(fs.exists(p))
			fs.delete(p, true);
		if(plan.intermediate.size()==0 && (plan.scans.size()!=0 || plan.resultScans.size()!=0)){//merge join map only job
			job = new Job(conf, "Merge Join");
			if(fs.exists(out))
				fs.delete(out, true);
			FileOutputFormat.setOutputPath(job,out);
			job.setJarByClass(MapReduceJoinExecutor.class);
			job.setMapperClass(MergeJoinMapper.class);
			
		    job.setOutputKeyClass(Bindings.class);
		    job.setOutputValueClass(BytesWritable.class);
		    
			List<Scan> inscans = new ArrayList<Scan>();

			int pat=0,group=0;
			int jvar =visitor.varRevIds.get(joinVar);//OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
			for(BGP e : plan.scans){
				if(plan.maxPattern.getClass().equals(IndexScan.class) && e.equals(((IndexScan)plan.maxPattern).bgp)){
					List<Scan> sc = e.getScans("?"+joinVar.getVarName());
					for(Scan s : sc){
						byte[] b1 = new byte[1];
						b1[0]=(byte)(int)visitor.varRevIds.get(joinVar);//OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
						s.setAttribute("joinVar", b1);
						s.setAttribute("stat0", Bytes.toBytes(e.getStatistics(joinVar)[0]));
						s.setAttribute("stat1", Bytes.toBytes(e.getStatistics(joinVar)[1]));
						s.setAttribute("group", Bytes.toBytes(group));
					}
					inscans.addAll(sc);
				}
				else{
					List<Scan> sc = e.getScans("?"+joinVar.getVarName());
					for(Scan s : sc){
						byte[] b1 = new byte[1];
						b1[0]=(byte)(int)visitor.varRevIds.get(joinVar);//OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
						s.setAttribute("joinVar", b1);
						b1 = new byte[1];
						b1[0]=(byte)pat;
						s.setAttribute("pattern", b1);
						s.setAttribute("stat0", Bytes.toBytes(e.getStatistics(joinVar)[0]));
						s.setAttribute("stat1", Bytes.toBytes(e.getStatistics(joinVar)[1]));
						s.setAttribute("group", Bytes.toBytes(group));
						
					    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
					    DataOutputStream dos = new DataOutputStream(out1);
					    s.write(dos);
					    job.getConfiguration().set("h2rdf.externalScans_"+pat, Base64.encodeBytes(out1.toByteArray()));
						pat++;

					}
					//externalscans.addAll(sc);
				}
				group++;
			}

			for(CachedResult cr : plan.resultScans){
				if(cr.equals(plan.maxPattern)){
					Scan s = cr.getScan();
					byte[] b1 = new byte[1];
					b1[0]=(byte)(int)visitor.varRevIds.get(joinVar);
					s.setAttribute("joinVar", b1);
					s.setAttribute("stat0", Bytes.toBytes(cr.getStatistics(visitor.varRevIds.get(joinVar))[0]));
					s.setAttribute("stat1", Bytes.toBytes(cr.getStatistics(visitor.varRevIds.get(joinVar))[1]));
					s.setAttribute("group", Bytes.toBytes(group));
					inscans.add(s);
					
				}
				else{
					Scan s = cr.getScan();
					byte[] b1 = new byte[1];
					b1[0]=(byte)(int)visitor.varRevIds.get(joinVar);
					s.setAttribute("joinVar", b1);
					b1 = new byte[1];
					b1[0]=(byte)pat;
					s.setAttribute("pattern", b1);
					s.setAttribute("stat0", Bytes.toBytes(cr.getStatistics(visitor.varRevIds.get(joinVar))[0]));
					s.setAttribute("stat1", Bytes.toBytes(cr.getStatistics(visitor.varRevIds.get(joinVar))[1]));
					s.setAttribute("group", Bytes.toBytes(group));
	
					
				    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
				    DataOutputStream dos = new DataOutputStream(out1);
				    s.write(dos);
				    job.getConfiguration().set("h2rdf.externalScans_"+pat, Base64.encodeBytes(out1.toByteArray()));
					pat++;
				}
				group++;
			}
			
			
			TableMapReduceUtil.initTableMapperJob(inscans, MergeJoinMapper.class
		    		, Bindings.class, NullWritable.class, job);
			
			System.out.println("h2rdf.inputPatterns     "+ pat);
			System.out.println("h2rdf.inputGroups     "+ group);
			job.getConfiguration().setInt("h2rdf.inputPatterns", pat);
			job.getConfiguration().setInt("h2rdf.inputGroups", group);
			
		    job.setInputFormatClass(MultiTableInputFormat.class);
		    
			job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
			//SequenceFileAsBinaryOutputFormat.setSequenceFileOutputKeyClass(job, Bindings.class);

			job.getConfiguration().setInt("dfs.block.size", 33554432);
		    
			//job.getConfiguration().setInt("mapred.map.tasks", 18);
			job.getConfiguration().setInt("mapred.reduce.tasks", 0);
			job.getConfiguration().setBoolean(
					"mapred.map.tasks.speculative.execution", false);
			job.getConfiguration().setBoolean(
					"mapred.reduce.tasks.speculative.execution", false);
			job.getConfiguration().setInt("io.sort.mb", 100);
			job.getConfiguration().setInt("io.file.buffer.size", 131072);
			job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
					
			
		    job.waitForCompletion(true);
		    
		}
		else{ //hybrid do the merge scan in reduce phase
			//Path partition = new Path("partition");
			Path partition =  new Path("partition/join_"+visitor.cachingExecutor.id+"_"+visitor.cachingExecutor.tid);

			partition = partition.makeQualified(fs);
	        if (fs.exists(partition)) {
	        	fs.delete(partition,true);
	        }
			SequenceFile.Writer projectionWriter = SequenceFile.createWriter(fs, conf, partition, ImmutableBytesWritable.class, NullWritable.class);
			
			List<Long> part= new ArrayList<Long>();
			
			System.out.println("max part length: "+plan.maxPartition.length);
			part.add(new Long(0));
			for (int i = 1; i < plan.maxPartition.length; i++) {
				part.add(plan.maxPartition[i][0]);
			}
			part.add(Long.MAX_VALUE);
			/*if(part.size()==2)
				part.add(1, new Long(234234));
			int pos=0;
			while(part.size()-1!=taskSlots){
				//pos = rand.nextInt(part.size()-1);
				if(part.size()-1<taskSlots){//split
					long s1 =part.get(pos);
					long s2 =part.get(pos+1);
					if(s2==Long.MAX_VALUE){
						if(s1==0)
							part.add(pos+1, (s1+s2)/2);
						else
							part.add(pos+1, s1+s1-part.get(pos-1));
					}
					else{
						part.add(pos+1, (s1+s2)/2);
					}
					pos+=2;
				}
				else{//merge
					if(pos==0)
						part.remove(pos+1);
					else
						part.remove(pos);
					pos++;
				}
				if(pos>=part.size()-1)
					pos=0;
				//System.out.println(part);
			}*/
			Collections.sort(part);
			System.out.println(part);
			int taskSlots=part.size()-1;
			/*for (int i = 1; i < plan.maxPartition.length; i++) {
				//System.out.println(plan.maxPartition[i][0]);
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(plan.maxPartition[i][0]);
				byte[] b = v.getBytesWithPrefix();
				projectionWriter.append(new ImmutableBytesWritable(b), NullWritable.get());
			}*/
			for (int i = 1; i < part.size()-1; i++) {
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(part.get(i));
				byte[] b = v.getBytesWithPrefix();
				projectionWriter.append(new ImmutableBytesWritable(b), NullWritable.get());
			}
			
			projectionWriter.close();
			
			job = new Job(conf, "SortMerge Join");
			if(fs.exists(out))
				fs.delete(out, true);
			
			job.setPartitionerClass(TotalOrderPartitioner.class);
			partition = partition.makeQualified(fs);
			TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partition);
			
			job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);

			int pat=0,group=0;
			for(BGP b: plan.scans){
				List<Scan> sc = b.getScans("?"+joinVar.getVarName());
				for(Scan s : sc){
					byte[] b1 = new byte[1];
					b1[0]=(byte)(int)visitor.varRevIds.get(joinVar);//OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
					s.setAttribute("joinVar", b1);
					b1 = new byte[1];
					b1[0]=(byte)pat;
					s.setAttribute("pattern", b1);
					s.setAttribute("stat0", Bytes.toBytes(b.getStatistics(joinVar)[0]));
					s.setAttribute("stat1", Bytes.toBytes(b.getStatistics(joinVar)[1]));
					s.setAttribute("group", Bytes.toBytes(group));
					
				    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
				    DataOutputStream dos = new DataOutputStream(out1);
				    s.write(dos);
				    job.getConfiguration().set("h2rdf.externalScans_"+pat, Base64.encodeBytes(out1.toByteArray()));
					pat++;
				}
				group++;
			}

			for(CachedResult cr : plan.resultScans){
				Scan s = cr.getScan();
				byte[] b1 = new byte[1];
				b1[0]=(byte)(int)visitor.varRevIds.get(joinVar);
				s.setAttribute("joinVar", b1);
				b1 = new byte[1];
				b1[0]=(byte)pat;
				s.setAttribute("pattern", b1);
				s.setAttribute("stat0", Bytes.toBytes(cr.getStatistics(visitor.varRevIds.get(joinVar))[0]));
				s.setAttribute("stat1", Bytes.toBytes(cr.getStatistics(visitor.varRevIds.get(joinVar))[1]));
				s.setAttribute("group", Bytes.toBytes(group));

				
			    ByteArrayOutputStream out1 = new ByteArrayOutputStream();
			    DataOutputStream dos = new DataOutputStream(out1);
			    s.write(dos);
			    job.getConfiguration().set("h2rdf.externalScans_"+pat, Base64.encodeBytes(out1.toByteArray()));
				pat++;
				group++;
			}
			System.out.println("h2rdf.scanPatterns     "+ pat);
			System.out.println("h2rdf.inputGroups     "+ group);
			job.getConfiguration().setInt("h2rdf.scanPatterns", pat);
			job.getConfiguration().setInt("h2rdf.inputGroups", group);
			job.getConfiguration().set("h2rdf.table", Bytes.toString(table.getTableName()));
			job.getConfiguration().set("h2rdf.joinVar", visitor.varRevIds.get(joinVar)+"");//OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
			
			pat=0;
			for(ResultBGP b :plan.intermediate){
				FileInputFormat.addInputPath(job, b.path);
			    Path t = b.path.makeQualified(fs);
			    System.out.println("h2rdf.inputFiles_"+t.toUri().toString()+"  "+ pat);
			    job.getConfiguration().setInt("h2rdf.inputFiles_"+t.toUri().toString(), pat);
			    System.out.println("h2rdf.inputFilesVar_"+t.toUri().toString()+"  "+ visitor.varRevIds.get(joinVar)+"");//OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
				job.getConfiguration().set("h2rdf.inputFilesVar_"+t.toUri().toString(), visitor.varRevIds.get(joinVar)+"");//OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
				
			    pat++;
				if(b.varRelabeling!=null){
					System.out.println("h2rdf.inputFilesHasRelabeling_"+t.toUri().toString()+"  "+ true);
					job.getConfiguration().setBoolean("h2rdf.inputFilesHasRelabeling_"+t.toUri().toString(), true);
					System.out.println("h2rdf.inputFilesRelabelingSize_"+t.toUri().toString()+"  "+ b.varRelabeling.size());
					job.getConfiguration().setInt("h2rdf.inputFilesRelabelingSize_"+t.toUri().toString(), b.varRelabeling.size());
					for(Entry<Integer,Integer> e : b.varRelabeling.entrySet()){
						System.out.println("h2rdf.inputFilesRelabeling_"+t.toUri().toString()+"_"+e.getKey()+"  "+ e.getValue());
						job.getConfiguration().setInt("h2rdf.inputFilesRelabeling_"+t.toUri().toString()+"_"+e.getKey(), e.getValue());
					}
				}
				else{
					System.out.println("h2rdf.inputFilesHasRelabeling_"+t.toUri().toString()+"  "+ false);
					job.getConfiguration().setBoolean("h2rdf.inputFilesHasRelabeling_"+t.toUri().toString(), false);
				}
				if(b.selectiveBindings!=null){
					System.out.println("h2rdf.inputFilesHasSelective_"+t.toUri().toString()+"  "+ true);
					job.getConfiguration().setBoolean("h2rdf.inputFilesHasSelective_"+t.toUri().toString(), true);
					System.out.println("h2rdf.inputFilesSelectiveSize_"+t.toUri().toString()+"  "+ b.selectiveBindings.size());
					job.getConfiguration().setInt("h2rdf.inputFilesSelectiveSize_"+t.toUri().toString(), b.selectiveBindings.size());
					int k=0;
					for(Entry<Integer,Long> e : b.selectiveBindings.entrySet()){
						System.out.println("h2rdf.inputFilesSelective_"+t.toUri().toString()+"_"+k+"  "+e.getKey()+"_"+ e.getValue());
						job.getConfiguration().set("h2rdf.inputFilesSelective_"+t.toUri().toString()+"_"+k, e.getKey()+"_"+e.getValue());
						k++;
					}
				}
				else{
					System.out.println("h2rdf.inputFilesHasSelective_"+t.toUri().toString()+"  "+ false);
					job.getConfiguration().setBoolean("h2rdf.inputFilesHasSelective_"+t.toUri().toString(), false);
				}
				//files.add(temp.path.toUri().toString());
				//conf.setInt("h2rdf.inputPatterns_"+pat, numpat);
			}
			System.out.println("h2rdf.inputPatterns     "+ pat);
			job.getConfiguration().setInt("h2rdf.inputPatterns", pat);

		    

			job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
			FileOutputFormat.setOutputPath(job,out);
			job.setJarByClass(MapReduceMergeJoinExecutor1.class);
			job.setMapperClass(SortMergeJoinMapper.class);
			job.setReducerClass(SortMergeJoinReducer.class);
			
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		    job.setMapOutputValueClass(Bindings.class);
		    job.setOutputKeyClass(Bindings.class);
		    job.setOutputValueClass(BytesWritable.class);
		    
		    job.getConfiguration().setInt("mapred.reduce.tasks", taskSlots);
			job.getConfiguration().setBoolean(
					"mapred.map.tasks.speculative.execution", false);
			job.getConfiguration().setBoolean(
					"mapred.reduce.tasks.speculative.execution", false);
			job.getConfiguration().setInt("io.sort.mb", 100);
			job.getConfiguration().setInt("io.file.buffer.size", 131072);
			job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
					
			
		    job.waitForCompletion(true);
		    
			
			
		}
		

		long countSample = job.getCounters().findCounter("h2rdf", "sample").getValue();
		System.out.println("countSample: "+countSample);
		Iterator<Counter> it = job.getCounters().getGroup("h2rdf").iterator();
		Map<Integer,double[]> stats = new HashMap<Integer, double[]>();
		while(it.hasNext()){
			Counter c = it.next();
			if(c.getName().equals("sample"))
				continue;
			double[] r = new double[2];
			r[0]=c.getValue();
			r[1]=1;
			stats.put(Integer.parseInt(c.getName()), r);
			System.out.println(c.getName()+" "+c.getValue());
			
		}
		
		System.out.println("Find Result Partitions");
		HashMap<Integer, List<List<Long>>> partialPartitions = new HashMap<Integer, List<List<Long>>>();
		FileStatus[] st = fs.listStatus(new Path("resultPartitions/"+outname));
		for(int i=0; i<st.length; i++){
			//System.out.println(st[i].getPath().getName());
			FSDataInputStream in = fs.open(st[i].getPath());
			try{
				while(in.available()>0){
					String line = in.readUTF();
					//System.out.println(line);
	            	String[] str1 = line.split("_");
	            	int var = Integer.parseInt(str1[0]);
	            	List<Long> l = new ArrayList<Long>();
	            	for (int j = 1; j < str1.length; j++) {
	            		l.add(Long.parseLong(str1[j]));
					}
	            	//System.out.println("Var: "+var+" "+l);
	            	List<List<Long>> r = partialPartitions.get(var);
	            	if(r==null){
	            		r = new ArrayList<List<Long>>();
	            		partialPartitions.put(var, r);
	            	}
	            	r.add(l);
				}
			}catch (EOFException e) {
				// TODO: handle exception
			}
			in.close();
		}
		HashMap<Integer, long[][]> resutlPartitions = new HashMap<Integer, long[][]>();
		for( Entry<Integer, List<List<Long>>> e : partialPartitions.entrySet()){
			//choose max partition to do more sophisticated partition
			List<Long> finalPart = new ArrayList<Long>();
			for(List<Long> part : e.getValue()){
				finalPart.addAll(part);
			}
			Collections.sort(finalPart);
			int i =0;
			List<Long> finalPart1 = new ArrayList<Long>();
			while(i<finalPart.size()){
				finalPart1.add(finalPart.get(i));
				i+=30;
			}
			
			System.out.println("Var: "+ e.getKey()+" "+finalPart1);
			long[][] val = new long[finalPart1.size()+1][1];
			i =1;
			val[0][0]=0;
			for(Long l : finalPart1){
				val[i][0]=l;
				i++;
			}
			resutlPartitions.put(e.getKey(), val);
		}
        //System.out.println(resutlPartitions);
		
		if(countSample==0){
			//OptimizeOpVisitorMergeJoin.finished=true;
		}
		
		return resultPlan(plan,out, stats,resutlPartitions);
	}
	
	private static List<ResultBGP> resultPlan(MergeJoinPlan plan, Path out, Map<Integer, double[]> stats, HashMap<Integer, long[][]> resutlPartitions) throws IOException {
		List<ResultBGP> ret = new ArrayList<ResultBGP>();
    	Set<Var> vars = new HashSet<Var>();
		for(BGP e:plan.scans){
			vars.addAll(e.joinVars);
		}
		for(ResultBGP e:plan.intermediate){
			vars.addAll(e.joinVars);
		}
		Map<Integer, double[]> newStats = new HashMap<Integer, double[]>();
		for(Entry<Integer, double[]> e1 : stats.entrySet()){
			double[] st = new double[2];
			st[0]=e1.getValue()[0];
			st[1]=0;
			for(Entry<Integer, double[]> e2 : stats.entrySet()){
				st[1]+=e2.getValue()[0]/e1.getValue()[0];
			}
			newStats.put(e1.getKey(), st);
		}
		ResultBGP r = new ResultBGP(vars, out, newStats);
		r.setPartitions(resutlPartitions);
    	ret.add(r);
    	
		return ret;
	}

}
