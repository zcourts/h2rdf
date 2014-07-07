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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.WriterFactory;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class CentralizedOrderingExecutor {

	public static List<ResultBGP> execute(ResultBGP resultBGP, long[][] maxPartition, List<Integer> orderVarsInt, String table, OptimizeOpVisitorDPCaching visitor) throws IOException, InterruptedException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path out =  new Path("output/join_"+visitor.cachingExecutor.id+"_"+visitor.cachingExecutor.tid);
		int countStats=0;
		Map<Integer,double[]> stats = new HashMap<Integer, double[]>();
		System.out.println("Sort");
		System.out.println(out.toUri());
		
		
		int pat=0, count2=0;
		System.out.println("Ordering: "+orderVarsInt);
		
		Map<byte[],List<Bindings>> map = new TreeMap<byte[], List<Bindings>>(Bytes.BYTES_COMPARATOR);
		pat++;
		if(fs.isDirectory(resultBGP.path)){
	    	FileStatus[] fss = fs.listStatus(resultBGP.path);
	        for (FileStatus status : fss) {
	            Path path = status.getPath();
	            if(path.getName().startsWith("part")){
	            	try{
				    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

				    	Bindings key = new Bindings();
						if(resultBGP.varRelabeling!=null && resultBGP.selectiveBindings!=null){
							key = new Bindings(resultBGP.varRelabeling, resultBGP.selectiveBindings);
						}
						else if(resultBGP.selectiveBindings!=null){
							key = new Bindings(resultBGP.selectiveBindings,0);
						}
						else if(resultBGP.varRelabeling!=null){
							key = new Bindings(resultBGP.varRelabeling);
						}
				    	while(reader.next(key)){
				    		if(!key.valid)
				    			continue;
				    		Bindings kb = new Bindings();
				    		for(Integer i : orderVarsInt){
				    			kb.map.put((byte)(int)i, key.map.remove((byte)(int)i));
				    		}
				    		Combinations c = new Combinations(kb, orderVarsInt);
				    		byte[] s;
				    		while((s = c.next())!=null){
								List<Bindings> v = map.get(s);
								if(v==null){
									v=new ArrayList<Bindings>();
									Bindings tk = key.clone();
									v.add(tk);
									map.put(s, v);
								}
								else{
									Bindings.mergeSamePattern(v, key.clone());
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
		    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, resultBGP.path, conf);
		    	Bindings key = new Bindings();
				if(resultBGP.varRelabeling!=null && resultBGP.selectiveBindings!=null){
					key = new Bindings(resultBGP.varRelabeling, resultBGP.selectiveBindings);
				}
				else if(resultBGP.selectiveBindings!=null){
					key = new Bindings(resultBGP.selectiveBindings,0);
				}
				else if(resultBGP.varRelabeling!=null){
					key = new Bindings(resultBGP.varRelabeling);
				}
		    	while(reader.next(key)){
		    		//System.out.println("Read: "+key.map);
		    		if(!key.valid)
		    			continue;
		    		Bindings kb = new Bindings();
		    		for(Integer i : orderVarsInt){
		    			kb.map.put((byte)(int)i, key.map.remove((byte)(int)i));
		    		}
		    		Combinations c = new Combinations(kb, orderVarsInt);
		    		byte[] s;
		    		while((s = c.next())!=null){
						List<Bindings> v = map.get(s);
						if(v==null){
							v=new ArrayList<Bindings>();
							Bindings tk = key.clone();
							v.add(tk);
							map.put(s, v);
						}
						else{
							Bindings.mergeSamePattern(v, key.clone());
						}
		    		}
		    	}
				
		    	reader.close();
        	}
        	catch (EOFException e1) {
        		System.out.println("empty");
			}
		}
		//System.out.println("Read keys: "+count2);
		
		Job job = new Job(conf, "Sort");
		if(fs.exists(out))
			fs.delete(out, true);
		job.setOutputFormatClass(HFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job,out);

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
	    job.getConfiguration().setInt("mapred.reduce.tasks", 1);
	    
		HFileOutputFormat outformat = new HFileOutputFormat();
		TaskAttemptContext context = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID());
		RecordWriter<ImmutableBytesWritable, KeyValue> recordWriter = outformat.getRecordWriter(context);
		
		for(Entry<byte[],List<Bindings>> e : map.entrySet()){
			/*InputStream is = new ByteArrayInputStream(e.getKey());
			for (int i = 0; i < orderVarsInt.size(); i++) {
				long t =SortedBytesVLongWritable.readLong(is);
				System.out.print(t+",");
				double[] v = stats.get(orderVarsInt.get(i));
				if(v==null){
					double[] st = new double[2];
					st[0]=1;
					st[1]=1;
					stats.put(orderVarsInt.get(i), st);
				}
				else{
					v[0]++;
				}
			}
			System.out.println();*/
			for(Integer i : orderVarsInt){
				double[] st = stats.get(i);
				if(st==null){
					st = new double[2];
					st[0]=1;
					st[1]=1;
					stats.put(i, st);
				}
				else{
					st[0]++;
				}
			}
			
			short q=0;
			ByteArrayOutputStream outStream = new ByteArrayOutputStream(); 
			DataOutputStream out1 = new DataOutputStream(outStream);
			for(Bindings b : e.getValue()){
				b.write(out1);
				//System.out.println(b.map);
				for(Entry<Byte, Set<Long>> e1 : b.map.entrySet()){
					double[] st = stats.get(e1.getKey().intValue());
					if(st==null){
						st = new double[2];
						st[0]=e1.getValue().size();
						st[1]=1;
						stats.put(e1.getKey().intValue(), st);
					}
					else{
						st[0]+=e1.getValue().size();
					}
				}
				countStats++;
				
			}
			out1.flush();
			KeyValue emmitedValue = new KeyValue(e.getKey().clone(), Bytes.toBytes("I"), Bytes.toBytes(q), outStream.toByteArray().clone());

			recordWriter.write(new ImmutableBytesWritable(e.getKey().clone()), emmitedValue);
		}
		recordWriter.close(context);
		outformat.getOutputCommitter(context).commitTask(context);
		
		return resultPlan(resultBGP,out,stats);
	}

	private static List<ResultBGP> resultPlan(ResultBGP resultBGP, Path out, Map<Integer, double[]> stats) throws IOException {
		List<ResultBGP> ret = new ArrayList<ResultBGP>();
    	Set<Var> vars = resultBGP.joinVars;
		
		long sum =0;
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
