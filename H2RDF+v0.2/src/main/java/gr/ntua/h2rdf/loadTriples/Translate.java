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
package gr.ntua.h2rdf.loadTriples;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class Translate {
	
	public static Job createSubmittableJob(String[] args) throws IOException {
		
		Job job = new Job();
		
		Configuration conf = job.getConfiguration();
		FileSystem fs;
		int reducers=0;
		try {
			fs = FileSystem.get(conf);
			FileStatus[] p = fs.listStatus(new Path("blockIds/"));
			reducers = p.length;
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setNumReduceTasks(reducers);
			
			Path out = new Path("translations");
			if (fs.exists(out)) {
				fs.delete(out,true);
			}
			FileOutputFormat.setOutputPath(job, out);
			FileInputFormat.addInputPath(job, new Path("temp"));

		    FileOutputFormat.setCompressOutput(job, true);
		    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		    
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(ImmutableBytesWritable.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(ImmutableBytesWritable.class);
			job.setOutputFormatClass( SequenceFileOutputFormat.class );
			job.setJarByClass(Translate.class);
			
			job.setMapperClass(Map.class);
			job.setReducerClass( Reduce.class );
	
			job.setPartitionerClass(IdPartitioner.class);
			
			job.setJobName( "Translate" );
		    job.getConfiguration().set("mapred.compress.map.output","true");
		    job.getConfiguration().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
			job.getConfiguration().setBoolean(
					"mapred.map.tasks.speculative.execution", false);
			job.getConfiguration().setBoolean(
					"mapred.reduce.tasks.speculative.execution", false);
			job.getConfiguration().setInt("io.sort.mb", 100);
			job.getConfiguration().setInt("io.file.buffer.size", 131072);
			job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
			job.getConfiguration().setInt("hbase.hregion.max.filesize", 67108864);
			//job.getConfiguration().setInt("hbase.hregion.max.filesize", 33554432);
			
			
		 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return job;
	}
	
   public static class IdPartitioner<VALUE> extends Partitioner<ImmutableBytesWritable, VALUE> implements Configurable {
	   
	public IdPartitioner() { }
	
	@Override
	public int getPartition(ImmutableBytesWritable key, VALUE value, int numPartitions) {

		SortedBytesVLongWritable t = new SortedBytesVLongWritable();
		t.setBytesWithPrefix(key.get());
		long l = t.getLong();
		ImmutableBytesWritable v = (ImmutableBytesWritable) value;
		byte[] b = v.get();
		ByteArrayInputStream bin = new ByteArrayInputStream(b);

		try {
			long val = SortedBytesVLongWritable.readLong(bin);
			SortedBytesVLongWritable v1 = new SortedBytesVLongWritable(val);
			v.set(v1.getBytesWithPrefix());
			byte[] v2 = new byte[bin.available()];
			bin.read(v2, 0, bin.available());
			key.set(v2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return Integer.parseInt((-(l+1))+"");
	}


	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		
	}
	   
   }
   
   public static class Map extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
	    
		public void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException {
			try {
				byte[] b = value.get();
				ByteArrayInputStream bin = new ByteArrayInputStream(b);
				long id = SortedBytesVLongWritable.readLong(bin);
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(id);
				byte[] i =v.getBytesWithPrefix();
				byte[] v1=new byte[i.length+key.get().length];
				System.arraycopy(i, 0, v1, 0, i.length);
				System.arraycopy(key.get(), 0, v1, i.length, key.get().length);
				//System.out.print(Bytes.toString(key.get())+" "+v.getLong() +" ");
				
				long block=0;
				ImmutableBytesWritable val = new ImmutableBytesWritable();
				while(true){
					block = SortedBytesVLongWritable.readLong(bin);
					v = new SortedBytesVLongWritable(block);
					//System.out.print(v.getLong()+" ");
					context.write(new ImmutableBytesWritable(v.getBytesWithPrefix()), new ImmutableBytesWritable(v1));
				}
				
			} catch (IOException e) {
				//System.out.println();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
 	
  }
   
   public static class Reduce extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

	public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException {
		try {
			ImmutableBytesWritable v = values.iterator().next();
			SortedBytesVLongWritable v1 = new SortedBytesVLongWritable();
			v1.setBytesWithPrefix(v.get());
			//System.out.println(Bytes.toString(key.get())+"    "+v1.getLong());
			context.write(key, v);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

  }
}
