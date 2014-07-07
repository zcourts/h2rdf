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
import gr.ntua.h2rdf.loadTriples.TotalOrderPartitioner;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class MROrderingExecutor {

	public static List<ResultBGP> execute(ResultBGP resultBGP, long[][] maxPartition, List<Integer> orderVarsInt, String table, OptimizeOpVisitorDPCaching visitor) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path out =  new Path("output/join_"+visitor.cachingExecutor.id+"_"+visitor.cachingExecutor.tid);

		Path partition =  new Path("partition/join_"+visitor.cachingExecutor.id+"_"+visitor.cachingExecutor.tid);
		
        if (fs.exists(partition)) {
        	fs.delete(partition,true);
        }
        SequenceFile.Writer projectionWriter = SequenceFile.createWriter(fs, conf, partition, ImmutableBytesWritable.class, NullWritable.class);
		
		List<Long> part= new ArrayList<Long>();
		
		System.out.println("max part length: "+maxPartition.length);
		//part.add(new Long(0));
		//for (int i = 1; i < maxPartition.length; i++) {
		//	part.add(maxPartition[i][0]);
		//}
		//part.add(Long.MAX_VALUE);
		part.add(new Long(0));
		for (int i = 1; i < maxPartition.length; i++) {
			part.add(maxPartition[i][0]);
		}
		part.add(Long.MAX_VALUE);
		TreeSet<Long> part1 = new TreeSet<Long>();
		for(Long l : part){
			part1.add(l);
		}
		part = new ArrayList<Long>();
		for(Long l : part1){
			part.add(l);
		}
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
			System.out.println(part);
		}*/
		System.out.println(part);
		Collections.sort(part);
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

		Job job = new Job(conf, "Sort");
		if(fs.exists(out))
			fs.delete(out, true);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		partition = partition.makeQualified(fs);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partition);
		job.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, resultBGP.path);
		FileOutputFormat.setOutputPath(job,out);
	    Path t = resultBGP.path.makeQualified(fs);
	    int pat=0;
		String ordering = "";
		for(Integer i : orderVarsInt){
			ordering+=i+"_";
		}
	    System.out.println("h2rdf.inputFiles_"+t.toUri().toString()+"  "+ pat);
	    job.getConfiguration().setInt("h2rdf.inputFiles_"+t.toUri().toString(), pat);
	    System.out.println("h2rdf.inputFilesVar_"+t.toUri().toString()+"  "+ ordering);//OptimizeOpVisitorMergeJoin.varIds.get(joinVar));
		job.getConfiguration().set("h2rdf.inputFilesVar_"+t.toUri().toString(), ordering);//OptimizeOpVisitorMergeJoin.varIds.get(joinVar));

	    job.getConfiguration().set("h2rdf.joinVar", ordering);
	    
		job.getConfiguration().setBoolean("h2rdf.inputFilesHasSelective_"+t.toUri().toString(), false);
		job.getConfiguration().setBoolean("h2rdf.inputFilesHasRelabeling_"+t.toUri().toString(), false);
		
		job.setJarByClass(MROrderingExecutor.class);
		job.setMapperClass(SortMergeJoinMapper.class);
		job.setReducerClass(OrderingReducer.class);
		
	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setMapOutputValueClass(Bindings.class);
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
      	job.setOutputValueClass(KeyValue.class);
      	
      	StringBuilder compressionConfigValue = new StringBuilder();
	    compressionConfigValue.append(URLEncoder.encode("I", "UTF-8"));
	    compressionConfigValue.append('=');
	    compressionConfigValue.append(URLEncoder.encode(Algorithm.SNAPPY.getName(), "UTF-8"));
        compressionConfigValue.append('&');
	    compressionConfigValue.append(URLEncoder.encode("S", "UTF-8"));
	    compressionConfigValue.append('=');
	    compressionConfigValue.append(URLEncoder.encode(Algorithm.SNAPPY.getName(), "UTF-8"));
        compressionConfigValue.append('&');
	    compressionConfigValue.append(URLEncoder.encode("T", "UTF-8"));
	    compressionConfigValue.append('=');
	    compressionConfigValue.append(URLEncoder.encode(Algorithm.SNAPPY.getName(), "UTF-8"));
	    job.getConfiguration().set("hbase.hfileoutputformat.families.compression", compressionConfigValue.toString());
      	

	    job.getConfiguration().setInt("mapred.reduce.tasks", taskSlots);
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
		job.getConfiguration().setInt("io.sort.mb", 100);
		job.getConfiguration().setInt("io.file.buffer.size", 131072);
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);

	    job.getConfiguration().set("mapred.compress.map.output","true");
	    job.getConfiguration().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		//job.getConfiguration().setInt("hbase.hregion.max.filesize", 268435456);
		job.getConfiguration().setInt("hbase.hregion.max.filesize", 67108864);
		//job.getConfiguration().setInt("hbase.hregion.max.filesize", 33554432);

	    job.waitForCompletion(true);

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
		
		
		//loadHFiles(out, table);
		
		//fs.delete(out,true);
		List<ResultBGP> ret = new ArrayList<ResultBGP>();
    	Set<Var> vars = new HashSet<Var>();
		vars.addAll(resultBGP.joinVars);

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
    	ret.add(new ResultBGP(vars, out, newStats));
		
		return ret;
	}

}
