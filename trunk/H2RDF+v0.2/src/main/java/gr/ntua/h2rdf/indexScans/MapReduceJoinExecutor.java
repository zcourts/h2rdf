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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
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

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.inputFormat2.FileTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.MultiTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.TableMapReduceUtil;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.openrdf.query.algebra.evaluation.function.string.Concat;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitor;
import com.hp.hpl.jena.sparql.core.Var;

public class MapReduceJoinExecutor {
	

	public static List<ResultBGP> execute(Map<Var, JoinPlan> m, HTable table,
			HTable indexTable) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Join");
		job.setJarByClass(MapReduceJoinExecutor.class);
		job.setMapperClass(JoinBGPMapper.class);
		job.setReducerClass(JoinBGPReducer.class);
		
	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    job.setMapOutputValueClass(Bindings.class);
	    job.setOutputKeyClass(Bindings.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
		List<Scan> scans = new ArrayList<Scan>();

		FileSystem fs=FileSystem.get(conf);
		int pat=0;
		Set<String> files = new HashSet<String>();
		for(Entry<Var, JoinPlan> e : m.entrySet()){
			int b;
			b=OptimizeOpVisitor.varIds.get(e.getKey());
			Iterator<ResultBGP> it =  e.getValue().Map.iterator();
			int numpat=0;
			while(it.hasNext()){
				ResultBGP temp = it.next();
				if(temp.getClass().equals(BGP.class)){
					List<Scan> sc = ((BGP)temp).getScans("?"+e.getKey().getVarName());
					for(Scan s : sc){
						byte[] b1 = new byte[1];
						b1[0]=OptimizeOpVisitor.varIds.get(e.getKey());
						s.setAttribute("joinVar", b1);
						b1 = new byte[1];
						b1[0]=(byte)pat;
						s.setAttribute("pattern", b1);
					}
					scans.addAll(sc);
				}
				else{
				    FileTableInputFormat.addInputPath(job, temp.path);
				    Path t = temp.path.makeQualified(fs);
				    System.out.println("h2rdf.inputFiles_"+t.toUri().toString()+"  "+ pat);
				    System.out.println("h2rdf.inputFilesVar_"+t.toUri().toString()+"  "+ b);
					job.getConfiguration().setInt("h2rdf.inputFiles_"+t.toUri().toString(), pat);
					job.getConfiguration().setInt("h2rdf.inputFilesVar_"+t.toUri().toString(), b);
					//files.add(temp.path.toUri().toString());
					//conf.setInt("h2rdf.inputPatterns_"+pat, numpat);
					
				}
				pat++;
				numpat++;
			}
			System.out.println("h2rdf.inputPatterns_"+b+"     "+ numpat);
			job.getConfiguration().setInt("h2rdf.inputPatterns_"+b, numpat);
			it =  e.getValue().Reduce.iterator();
			numpat=0;
			while(it.hasNext()){
				ResultBGP temp = it.next();
				if(temp.getClass().equals(BGP.class)){
					List<Scan> sc = ((BGP)temp).getScans("?"+e.getKey().getVarName());
					for(Scan s : sc){
					    ByteArrayOutputStream out = new ByteArrayOutputStream();
					    DataOutputStream dos = new DataOutputStream(out);
					    s.write(dos);
						conf.set("h2rdf.reduceScans_"+b+"_"+numpat, Base64.encodeBytes(out.toByteArray()));
					}
					scans.addAll(sc);
				}
				
			}
			System.out.println(b);
		    MultipleOutputs.addNamedOutput(job, b+"",SequenceFileAsBinaryOutputFormat.class, Bindings.class, BytesWritable.class);
		}
		MultipleOutputs.setCountersEnabled(job, true);
		
	    job.setInputFormatClass(FileTableInputFormat.class);
	    if(scans.size()>0){
	    	FileTableInputFormat.initTableMapperJob(scans, JoinBGPMapper.class
	    		, ImmutableBytesWritable.class, Bindings.class, job);
	    }
	    
		Path out =  new Path("output/join_"+JoinExecutor.joinId);
		if(fs.exists(out))
			fs.delete(out, true);
		FileOutputFormat.setOutputPath(job,out);

	    
	    
		//job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
		//SequenceFileAsBinaryOutputFormat.setSequenceFileOutputKeyClass(job, Bindings.class);
	    
	    
		//job.getConfiguration().setInt("mapred.map.tasks", 18);
		//job.getConfiguration().setInt("mapred.reduce.tasks", 25);
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
		job.getConfiguration().setInt("io.sort.mb", 100);
		job.getConfiguration().setInt("io.file.buffer.size", 131072);
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
				
		
	    job.waitForCompletion(true);
	   
		return resultPlan(m,out, fs);
	}

	private static List<ResultBGP> resultPlan(Map<Var, JoinPlan> m, Path out, FileSystem fs) throws IOException {
		List<ResultBGP> ret = new ArrayList<ResultBGP>();
		Map<Integer,double[]> map = new HashMap<Integer, double[]>();
		for(Entry<Var, JoinPlan> e :m.entrySet()){
			
			JoinPlan plan = e.getValue();
	    	Set<Var> vars = new HashSet<Var>();
	    	
			for(ResultBGP b : plan.Map){
				vars.addAll(b.joinVars);
			}
			for(ResultBGP b : plan.Reduce){
				vars.addAll(b.joinVars);
			}
			Path p = new Path(out.toUri()+"/"+OptimizeOpVisitor.varIds.get(e.getKey())+"*");
			long sum =0;
			if(fs.isDirectory(out)){
		    	FileStatus[] fss = fs.listStatus(out);
		        for (FileStatus status : fss) {
		            Path path = status.getPath();
		            if(path.getName().startsWith(OptimizeOpVisitor.varIds.get(e.getKey())+""))
		            	sum+=fs.getContentSummary(path).getLength()/vars.size()/8;
		        }
	    	}
			System.out.println("Output size: "+sum);
			double[] r =new double[2];
			r[1]=1;
			r[0]=sum;
			int i=0;
			for(Var v: vars){
				map.put(i, r);
				i++;
			}
	    	ret.add(new ResultBGP(vars, p,map));
		}
		return ret;
	}
}
