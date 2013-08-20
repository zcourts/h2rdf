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

import gr.ntua.h2rdf.inputFormat2.FileTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.MultiTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.TableMapReduceUtil;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;
import gr.ntua.h2rdf.loadTriples.TotalOrderPartitioner;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitor;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;
import com.hp.hpl.jena.sparql.core.Var;

public class MapReduceMergeJoinExecutor {
	private static FileSystem fs;

	public static List<ResultBGP> execute(MergeJoinPlan plan, Var joinVar, HTable table,
			HTable indexTable) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		fs=FileSystem.get(conf);
		Job job=null;
		Path out =  new Path("output/join_"+JoinExecutor.joinId);
		if(plan.intermediate.size()==0 && plan.scans.size()!=0){//merge join map job
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
			int jvar =OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
			for(BGP e : plan.scans){
				if(!e.equals(plan.maxBGP)){
					List<Scan> sc = e.getScans("?"+joinVar.getVarName());
					for(Scan s : sc){
						byte[] b1 = new byte[1];
						b1[0]=OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
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
				else{
					List<Scan> sc = e.getScans("?"+joinVar.getVarName());
					for(Scan s : sc){
						byte[] b1 = new byte[1];
						b1[0]=OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
						s.setAttribute("joinVar", b1);
						s.setAttribute("stat0", Bytes.toBytes(e.getStatistics(joinVar)[0]));
						s.setAttribute("stat1", Bytes.toBytes(e.getStatistics(joinVar)[1]));
						s.setAttribute("group", Bytes.toBytes(group));
					}
					inscans.addAll(sc);
					
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
		else if(plan.scans.size()==0 && plan.intermediate.size()!=0){//hash join only
			
		}
		else{ //hybrid do the merge scan in reduce phase
			//Path partition = new Path("partition");
			Path partition = new Path("partition/join_"+JoinExecutor.joinId);
			
	        if (fs.exists(partition)) {
	        	fs.delete(partition,true);
	        }
			SequenceFile.Writer projectionWriter = SequenceFile.createWriter(fs, conf, partition, ImmutableBytesWritable.class, NullWritable.class);
			
			for (int i = 1; i < plan.maxPartition.length; i++) {
				//System.out.println(plan.maxPartition[i][0]);
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(plan.maxPartition[i][0]);
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
					b1[0]=OptimizeOpVisitorMergeJoin.varIds.get(joinVar);
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

			System.out.println("h2rdf.scanPatterns     "+ pat);
			System.out.println("h2rdf.inputGroups     "+ group);
			job.getConfiguration().setInt("h2rdf.scanPatterns", pat);
			job.getConfiguration().setInt("h2rdf.inputGroups", group);
			job.getConfiguration().set("h2rdf.table", Bytes.toString(table.getTableName()));
			job.getConfiguration().setInt("h2rdf.joinVar", OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
			
			pat=0;
			for(ResultBGP b :plan.intermediate){
				FileInputFormat.addInputPath(job, b.path);
			    Path t = b.path.makeQualified(fs);
			    System.out.println("h2rdf.inputFiles_"+t.toUri().toString()+"  "+ pat);
			    System.out.println("h2rdf.inputFilesVar_"+t.toUri().toString()+"  "+ OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
				job.getConfiguration().setInt("h2rdf.inputFiles_"+t.toUri().toString(), pat);
				job.getConfiguration().setInt("h2rdf.inputFilesVar_"+t.toUri().toString(), OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
				pat++;
				//files.add(temp.path.toUri().toString());
				//conf.setInt("h2rdf.inputPatterns_"+pat, numpat);
			}
			System.out.println("h2rdf.inputPatterns     "+ pat);
			job.getConfiguration().setInt("h2rdf.inputPatterns", pat);

		    

			job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
			FileOutputFormat.setOutputPath(job,out);
			job.setJarByClass(MapReduceJoinExecutor.class);
			job.setMapperClass(SortMergeJoinMapper.class);
			job.setReducerClass(SortMergeJoinReducer.class);
			
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		    job.setMapOutputValueClass(Bindings.class);
		    job.setOutputKeyClass(Bindings.class);
		    job.setOutputValueClass(BytesWritable.class);
		    
		    job.getConfiguration().setInt("mapred.reduce.tasks", plan.maxPartition.length);
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
		
		
		if(countSample==0){
			OptimizeOpVisitorMergeJoin.finished=true;
		}
		
		return resultPlan(plan,out, stats);
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
		/*long sum =0;
		if(fs.isDirectory(out)){
	    	FileStatus[] fss = fs.listStatus(out);
	        for (FileStatus status : fss) {
	            Path path = status.getPath();
	            if(path.getName().startsWith("part"))
	            	sum+=fs.getContentSummary(path).getLength()/vars.size()/4;
	        }
    	}
		System.out.println("Output size: "+sum);*/
    	ret.add(new ResultBGP(vars, out,stats));
		return ret;
	}

}
