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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Arrays;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;

public class SortIds {

	private static String TABLE_NAME ;
	public static int bucket = 5000000;
	
	public static Job createSubmittableJob(String[] args, Counters counters, int numReducers) throws IOException {
		//numReducers=52;
		Job job = new Job();
		TABLE_NAME = args[1];
		Configuration conf = job.getConfiguration();
		long sum=0, maxCommon=Integer.MAX_VALUE;
		try {
			HTable table = new HTable(HBaseConfiguration.create(), "Counters");
		
			for (int i = 1; i < numReducers; i++) {
				Get get = new Get(Bytes.toBytes("count."+i));
				get.addColumn(Bytes.toBytes("counter"), new byte[0]);
				Result res = table.get(get);
				if(!res.isEmpty()){
					long v=Bytes.toLong(res.raw()[0].getValue());
					//long v = counters.findCounter("Countergroup", "count."+i).getValue();
					if(v< maxCommon){
						maxCommon = v;
					}
					//conf.setLong("count."+i, v);
					//System.out.println(v);
					sum+=v;
				}
			}
			System.out.println("maxCommon: "+maxCommon);
			job.getConfiguration().setLong("count.MaxCommon", maxCommon);
			job.getConfiguration().setInt("count.numReducers", numReducers-1);
			job.getConfiguration().setInt("count.sum", (int) sum);
			
			Get get = new Get(Bytes.toBytes("count.chunks"));
			get.addColumn(Bytes.toBytes("counter"), new byte[0]);
			Result res = table.get(get);
			int stringReducers =0;
			if(!res.isEmpty()){
				stringReducers=(int)Bytes.toLong(res.raw()[0].getValue());
			}
			//int stringReducers = (int) counters.findCounter("Countergroup", "count.chunks").getValue();
			int intReducers = (int) Math.ceil((double)sum/(double)bucket);
			sum=maxCommon*(numReducers-1);
			for (int i = 1; i < numReducers; i++) {
				get = new Get(Bytes.toBytes("count."+i));
				get.addColumn(Bytes.toBytes("counter"), new byte[0]);
				res = table.get(get);
				if(!res.isEmpty()){
					long v=Bytes.toLong(res.raw()[0].getValue());
					//long v = counters.findCounter("Countergroup", "count."+i).getValue();
					job.getConfiguration().setLong("count."+(i-1), sum);
					//System.out.println("count."+i+" "+sum);
					sum+=v-maxCommon;
				}
				
				
			}
			
			
			System.out.println("stringReducers: "+stringReducers+" sum: "+sum+" intReducers: "+intReducers);
			
	
			job.getConfiguration().setInt("count.stringReducers", stringReducers);
			job.getConfiguration().setInt("count.intReducers", intReducers);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setNumReduceTasks(stringReducers+intReducers);
			
			Path out = new Path(args[1]);
			FileSystem fs;
			try {
				fs = FileSystem.get(conf);
				if (fs.exists(out)) {
					fs.delete(out,true);
				}
				if(fs.exists(new Path("temp")))
					fs.delete(new Path("temp"), true);
			} catch (IOException e) {
				e.printStackTrace();
			}
			FileOutputFormat.setOutputPath(job, out);
			FileInputFormat.addInputPath(job, new Path("uniqueIds"));
			FileInputFormat.addInputPath(job, new Path("blockIds"));
			
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(ImmutableBytesWritable.class);
			job.setOutputFormatClass( HFileOutputFormat.class );
			job.setJarByClass(SortIds.class);
			
			//configure compression
		    StringBuilder compressionConfigValue = new StringBuilder();
		    compressionConfigValue.append(URLEncoder.encode("1", "UTF-8"));
		    compressionConfigValue.append('=');
		    compressionConfigValue.append(URLEncoder.encode(Algorithm.GZ.getName(), "UTF-8"));
	        compressionConfigValue.append('&');
		    compressionConfigValue.append(URLEncoder.encode("2", "UTF-8"));
		    compressionConfigValue.append('=');
		    compressionConfigValue.append(URLEncoder.encode(Algorithm.GZ.getName(), "UTF-8"));
		    job.getConfiguration().set("hbase.hfileoutputformat.families.compression", compressionConfigValue.toString());

		    
		    job.getConfiguration().set("mapred.compress.map.output","true");
		    job.getConfiguration().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
			
		    
			job.setMapperClass(Map.class);
			job.setReducerClass( Reduce.class );
	
			job.setPartitionerClass(TwoTotalOrderPartitioner.class);
			TwoTotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("partition/stringIdPartition"));
			//job.setCombinerClass(Combiner.class);
			job.setJobName( "SortIds" );
			job.getConfiguration().setBoolean(
					"mapred.map.tasks.speculative.execution", false);
			job.getConfiguration().setBoolean(
					"mapred.reduce.tasks.speculative.execution", false);
			job.getConfiguration().setInt("io.sort.mb", 100);
			job.getConfiguration().setInt("io.file.buffer.size", 131072);
			job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
			//job.getConfiguration().setInt("hbase.hregion.max.filesize", 268435456);
			job.getConfiguration().setInt("hbase.hregion.max.filesize", 67108864);
			//job.getConfiguration().setInt("hbase.hregion.max.filesize", 33554432);
			
			
		 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return job;
	}
	
   public static class TwoTotalOrderPartitioner<VALUE> extends Partitioner<ImmutableBytesWritable, VALUE> implements Configurable {
	   
	TotalOrderPartitioner<ImmutableBytesWritable, VALUE> partitioner = null;
	
	public TwoTotalOrderPartitioner() { }
	
	public int stringReducers,intReducers, chunk;
	@Override
	public int getPartition(ImmutableBytesWritable key, VALUE value, int numPartitions) {
		byte k = key.get()[0];
		if(k==(byte)0){
			byte[] b = key.get();
			byte[] id = new byte[b.length-1];
			for (int i = 0; i < b.length-1; i++) {
				id[i]=b[i+1];
			}
			key.set(id);
			return partitioner.getPartition(key, value, stringReducers);
		}
		else{
			byte[] b = key.get();
			byte[] bb = new byte[b.length-1];
			for (int i = 0; i < bb.length; i++) {
				bb[i]=b[i+1];
			}
			key.set(bb);
			SortedBytesVLongWritable id = new SortedBytesVLongWritable();
			id.setBytesWithPrefix(bb);

			return stringReducers + (int)(id.getLong()/(long)chunk);
		}
	}

	public static void setPartitionFile(Configuration configuration,
			Path path) {
		TotalOrderPartitioner.setPartitionFile(configuration, path);
	}

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		partitioner = new TotalOrderPartitioner<ImmutableBytesWritable, VALUE>();
		partitioner.setConf(conf);
		stringReducers = conf.getInt("count.stringReducers", 0);
		intReducers = conf.getInt("count.intReducers", 0);
		int sum = conf.getInt("count.sum", 0);
		chunk = ((int)Math.ceil(((double)sum)/((double)intReducers)));
		
	}
	   
   }
   
   public static class Map extends Mapper<ImmutableBytesWritable, Writable, ImmutableBytesWritable, ImmutableBytesWritable> {
	    boolean projectionMapper=false;	
	    ImmutableBytesWritable projectionFile = new ImmutableBytesWritable();
	    long maxCommon = 0;
	    long[] count = null;
	    int numReducers=0, redId=0;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			FileSplit split = (FileSplit) context.getInputSplit();
			if(split.toString().contains("blockIds/")){
				System.out.println("ProjectionMapper");
				System.out.println("split: "+split.toString());
				projectionMapper=true;
				FileSystem fs;
				try {
					fs = FileSystem.get(context.getConfiguration());
					FileStatus[] p = fs.listStatus(new Path("blockIds/"));
					for (int i = 0; i < p.length; i++) {
						//System.out.println("split: "+split.getPath().getName());
						//System.out.println("file: "+p[i].getPath().getName());
						if(split.getPath().getName().contains(p[i].getPath().getName())){
							SortedBytesVLongWritable t = new SortedBytesVLongWritable(-(i+1));
							projectionFile.set(t.getBytesWithPrefix());
							System.out.println("Split Id: "+(-(i+1)));
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else{
				String n = split.getPath().getName();
				redId = Integer.parseInt(n.substring(n.lastIndexOf('-')+1))-1;
				numReducers = context.getConfiguration().getInt("count.numReducers", 0);
				count = new long[numReducers];
				for (int i = 0; i < numReducers; i++) {
					count[i] = context.getConfiguration().getInt("count."+i, 0);
					//System.out.println(count[i]);
				}
				maxCommon = context.getConfiguration().getLong("count.MaxCommon", 0);
				
				//System.out.println("maxCommon"+ maxCommon);
			}
		}
		
		public void map(ImmutableBytesWritable key, Writable value, Context context) throws IOException {
			try {
				if(projectionMapper){
					byte[] k = key.get();
					byte[] newkey = new byte[k.length+1];
					newkey[0]=(byte) 0;
					for (int i = 1; i < newkey.length; i++) {
						newkey[i]=k[i-1];
					}
					context.write(new ImmutableBytesWritable(newkey), projectionFile);
				}
				else{
					SortedBytesVLongWritable lid = new SortedBytesVLongWritable();
					lid.setBytesWithPrefix(((ImmutableBytesWritable) value).get());
					long locId = lid.getLong();
					long globId = 0;
					if(locId<maxCommon){
						globId = locId*numReducers+redId;
					}
					else{
						globId = count[redId]+(locId-maxCommon);
					}
					SortedBytesVLongWritable gid = new SortedBytesVLongWritable(globId);
					byte[] k = gid.getBytesWithPrefix();
					byte[] newkey = new byte[k.length+1];
					newkey[0]=(byte) 1;
					for (int i = 1; i < newkey.length; i++) {
						newkey[i]=k[i-1];
					}

					//System.out.println("locId: "+locId+" globId:"+globId);

					context.write(new ImmutableBytesWritable(newkey), new ImmutableBytesWritable(key.get()));
					
					k = key.get();
					newkey = new byte[k.length+1];
					newkey[0]=(byte) 0;
					for (int i = 1; i < newkey.length; i++) {
						newkey[i]=k[i-1];
					}

					context.write(new ImmutableBytesWritable(newkey), new ImmutableBytesWritable(gid.getBytesWithPrefix()));
					
					
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
 	
  }
   
   public static class Reduce extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {

	public int id=0, stringReducers=0, projections=0;;
	public String[] projectionName=null;
	public Writer writer;
	public SequenceFile.Writer projectionWriter = null;
	public FileSystem fs;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		fs = FileSystem.get(context.getConfiguration());
		String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
		id = Integer.parseInt(idStr[idStr.length-2]);
		stringReducers = context.getConfiguration().getInt("count.stringReducers", 0);
		if(id<stringReducers){
			Path path = new Path("temp/"+context.getConfiguration().get("mapred.task.id").split("_")[idStr.length-2]);
        	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, context.getConfiguration());
			writer = SequenceFile.createWriter(fs, context.getConfiguration(),
					path, ImmutableBytesWritable.class, ImmutableBytesWritable.class,SequenceFile.CompressionType.BLOCK, codec);
		}
	
	}

	public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException {
		try {
			if(id<stringReducers){
				ByteArrayOutputStream b = new ByteArrayOutputStream();
				byte[] id=null;
				for(ImmutableBytesWritable value : values) {
					SortedBytesVLongWritable t = new SortedBytesVLongWritable();
					t.setBytesWithPrefix(value.get());
					long i = t.getLong();
					if(i>=0){
						//System.out.print(Bytes.toString(key.get())+"  "+i+" ");
						KeyValue emmitedValue = new KeyValue(key.get(), Bytes
							.toBytes("1"), new byte[0], value.get());
						context.write(key, emmitedValue);
						id=value.get().clone();
					}
					else{
						SortedBytesVLongWritable t1 = new SortedBytesVLongWritable();
						t1.setBytesWithPrefix(value.get());
						//System.out.print(t1.getLong()+" ");
						b.write(value.get().clone());
					}
				}
				byte[] bytes = b.toByteArray();
				byte[] b2 = new byte[bytes.length+id.length];
				System.arraycopy(id, 0, b2, 0, id.length);
				System.arraycopy(bytes, 0, b2, id.length, bytes.length);
				writer.append(key, new ImmutableBytesWritable(b2));
				
			}
			else{
				SortedBytesVLongWritable t = new SortedBytesVLongWritable();
				t.setBytesWithPrefix(key.get());
				//System.out.println(Bytes.toStringBinary(key.get())+" "+t.getLong());
				KeyValue emmitedValue = new KeyValue(key.get(), Bytes
					.toBytes("2"), new byte[0], (values.iterator().next()).get());
				context.write(key, emmitedValue);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		if(id<stringReducers){
			writer.close();
		}
		super.cleanup(context);
	}



  }

public static void loadHFiles(String[] args)throws Exception {
		Configuration conf = new Configuration();
	    HBaseAdmin hadmin = new HBaseAdmin(conf);
	    
	    
	    
		Path hfofDir= new Path(args[1]);
		FileSystem fs = hfofDir.getFileSystem(conf);
	    //if (!fs.exists(hfofDir)) {
	    //  throw new FileNotFoundException("HFileOutputFormat dir " +
	    //      hfofDir + " not found");
	    //}
	    FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
	    //if (familyDirStatuses == null) {
	    //  throw new FileNotFoundException("No families found in " + hfofDir);
	    //}
	    int length =0;
	    byte[][] splits = new byte[18000][];
	    for (FileStatus stat : familyDirStatuses) {
	      if (!stat.isDir()) {
	        continue;
	      }
	      Path familyDir = stat.getPath();
	      // Skip _logs, etc
	      if (familyDir.getName().startsWith("_")) continue;
	      //byte[] family = familyDir.getName().getBytes();
	      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
	      for (Path hfile : hfiles) {
	        if (hfile.getName().startsWith("_")) continue;
		    HFile.Reader hfr = HFile.createReader(fs, hfile, new CacheConfig(conf));
		    //HFile.Reader hfr = 	new HFile.Reader(fs, hfile, null, false);
		    final byte[] first;
		    try {
		      hfr.loadFileInfo();
		      first = hfr.getFirstRowKey();
		    }  finally {
		      hfr.close();
		    }
		    splits[length]=first.clone();
		    length++;
	      }
	    }
	    //System.out.println(length);
	    
	    byte[][] splits1 = new byte[length][];

	    for (int i = 0; i < splits1.length; i++) {
	    		splits1[i]=splits[i];
		}
	    Arrays.sort(splits1, Bytes.BYTES_COMPARATOR);
		//HTableDescriptor desc = new HTableDescriptor("H2RDF");
		
	    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME+"_Index");
	    
		HColumnDescriptor family= new HColumnDescriptor("1");
		family.setCompressionType(Algorithm.GZ);
		desc.addFamily(family); 
		HColumnDescriptor family2= new HColumnDescriptor("2");
		family2.setCompressionType(Algorithm.GZ);
		desc.addFamily(family2); 
		//for (int i = 0; i < splits.length; i++) {
		//	System.out.println(Bytes.toStringBinary(splits[i]));
		//}
		conf.setInt("zookeeper.session.timeout", 600000);
		if(hadmin.tableExists(TABLE_NAME+"_Index")){
			//hadmin.disableTable(TABLE_NAME);
			//hadmin.deleteTable(TABLE_NAME);
		}
		else{
			hadmin.createTable(desc, splits1);
		}
		//hadmin.createTable(desc);
		String[] args1 = new String[2];
		args1[0]=args[1];
		args1[1]=TABLE_NAME+"_Index";
		//args1[1]="new2";
		
		ToolRunner.run(new LoadIncrementalHFiles(HBaseConfiguration.create()), args1);
	
}
   
}
