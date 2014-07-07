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

import gr.ntua.h2rdf.loadTriples.TotalOrderPartitioner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;



public class TranslateAndImport implements Tool {
	private static final int bucketSampledTriples = 3000000;//(gia 1% sampling) 650000(gia 10%) //32MB regions
	public static String TABLE_NAME;
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub

	}

	public Job createSubmittableJob(String[] args) throws IOException, ClassNotFoundException{
		//compute sample partitions
		FileSystem fs;
		Configuration conf = new Configuration();
    	int collected=0, chunks=0;
	    try {
	    	fs = FileSystem.get(conf);
	    	Path sampleDir= new Path("sample");
		    FileStatus[] samples = fs.listStatus(sampleDir);
			TreeSet<String> set = new TreeSet<String>();
		    for (FileStatus sample : samples) {
		    	FSDataInputStream in = fs.open(sample.getPath());
		    	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, conf);
	    		CompressionInputStream in1 = codec.createInputStream(in);
		    	NxParser nxp = new NxParser(in1);
		    	Iterator<Node[]> it = nxp.iterator();
		    	while(it.hasNext()){
		    		Node[] tr = it.next();
		    		//System.out.println(tr[0].toN3());
	            	set.add(tr[0].toN3());
	            	set.add(tr[1].toN3());
	            	set.add(tr[2].toN3());
		    	}
				in1.close();
				in.close();
		    }
		    
            IndexTranslator translator = new IndexTranslator(TABLE_NAME+"_Index");
            HashMap<String, Long> index = translator.translate(set);
            set.clear();
			TreeSet<ImmutableBytesWritable> set1 = new TreeSet<ImmutableBytesWritable>(new ImmutableBytesWritable.Comparator());

		    for (FileStatus sample : samples) {
		    	FSDataInputStream in = fs.open(sample.getPath());
		    	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, conf);
	    		CompressionInputStream in1 = codec.createInputStream(in);
		    	NxParser nxp = new NxParser(in1);
		    	Iterator<Node[]> it = nxp.iterator();
		    	while(it.hasNext()){
		    		Node[] tr = it.next();
		    		ByteTriple btr = new ByteTriple(index.get(tr[0].toN3()), index.get(tr[1].toN3()), index.get(tr[2].toN3()));
	            	set1.add(new ImmutableBytesWritable(btr.getSPOByte()));
	            	set1.add(new ImmutableBytesWritable(btr.getSOPByte()));
	            	set1.add(new ImmutableBytesWritable(btr.getOPSByte()));
	            	set1.add(new ImmutableBytesWritable(btr.getOSPByte()));
	            	set1.add(new ImmutableBytesWritable(btr.getPOSByte()));
	            	set1.add(new ImmutableBytesWritable(btr.getPSOByte()));
		    	}
				in1.close();
				in.close();
		    }
		    index.clear();

		    Path p = new Path("hexastorePartition");
	    	if (fs.exists(p)) {
	        	fs.delete(p,true);
	        }
	    	SequenceFile.Writer partitionWriter = SequenceFile.createWriter(fs, conf, p, ImmutableBytesWritable.class, NullWritable.class);
			
        	double chunkSize= bucketSampledTriples*DistinctIds.samplingRate;
      	  	System.out.println("chunkSize: "+chunkSize);
      	  	Iterator<ImmutableBytesWritable> it = set1.iterator();
      	  	while( it.hasNext() ) {
      	  		ImmutableBytesWritable key = it.next();
      	  		if( collected > chunkSize ) {
      	  			partitionWriter.append(key, NullWritable.get());
      	  			//System.out.println(Bytes.toStringBinary(key.get()));
      	  			collected=0;
      	  			chunks++;
      	  		}
      	  		else{
      	  			collected++;
      	  		}
      	  	}
      	  	System.out.println("chunks: "+chunks);
      	  	partitionWriter.close();
		    
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
		
		
		
		
		Job job = new Job();
		job = new Job(conf, "Import Hexastore");
      
     	FileInputFormat.setInputPaths(job, new Path(args[0]));
     	Path out = new Path("out");
	    try {
	    	fs = FileSystem.get(conf);
	        if (fs.exists(out)) {
	        	fs.delete(out,true);
	        }
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
      	FileOutputFormat.setOutputPath(job, out);

		job.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("hexastorePartition"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		

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
	    //job.getConfiguration().setInt("hbase.mapreduce.hfileoutputformat.blocksize",262144);
	    //job.getConfiguration().setInt("hbase.mapreduce.hfileoutputformat.blocksize",16384);
      	job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      	job.setMapOutputValueClass(NullWritable.class);
      	job.setOutputKeyClass(ImmutableBytesWritable.class);
      	job.setOutputValueClass(KeyValue.class);
      	job.setJarByClass(TranslateAndImport.class);
      	job.setMapperClass(Map.class);
      	//job.setReducerClass(HexaStoreHistogramsReduce.class);
      	job.setReducerClass(HexaStoreReduce.class);
      	

		job.getConfiguration().set("h2rdf.tableName", TABLE_NAME);
		job.getConfiguration().setInt("mapred.reduce.tasks", chunks+1);
      	//job.setCombinerClass(Combiner.class);
      	job.setJobName( "Translate Projections" );
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
		//job.getConfiguration().setInt("hbase.hregion.max.filesize", 67108864);
		job.getConfiguration().setInt("hbase.hregion.max.filesize", 33554432);
      
      	return job;
      
	}

	
	@Override
	public int run(String[] args) throws Exception {
		TABLE_NAME = args[1];
		Job job =createSubmittableJob(args);
		job.waitForCompletion(true);
		loadHFiles();
		
	    return 0;
	}

   public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, NullWritable> {
	   	private HashMap<String, Long> index;
	   	private NullWritable val = NullWritable.get();
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			FileSplit split = (FileSplit) context.getInputSplit();
			
			//System.out.println("blockIds/"+split.toString().substring(split.toString().lastIndexOf("/")+1));
			//String name =split.getPath().getName();
			//name.substring(name.lastIndexOf("/")+1);
			index = new HashMap<String, Long>();
			String n =split.toString().substring(split.toString().lastIndexOf("/")+1);
			n=n.replace(":", "_");
			n=n.replace("+", "_");
			System.out.println("split: "+n);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FileStatus[] p = fs.listStatus(new Path("blockIds/"));
			for (int i = 0; i < p.length; i++) {
				//System.out.println("file: "+p[i].getPath().getName());
				if(n.contains(p[i].getPath().getName())){
					String id = i+"";
					while(id.length()<5){
						id="0"+id;
					}
					Path translation = new Path("translations/part-r-"+id);
					System.out.println(translation.getName());
					SequenceFile.Reader reader = new SequenceFile.Reader(fs, translation, context.getConfiguration());
			    	ImmutableBytesWritable key = new ImmutableBytesWritable();
			    	ImmutableBytesWritable value = new ImmutableBytesWritable();
			    	while(reader.next(key)){
			    		reader.getCurrentValue(value);
			    		SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			    		v.setBytesWithPrefix(value.get());
			    		//System.out.println(Bytes.toString(key.get()));
			    		index.put(Bytes.toString(key.get()), v.getLong());
			    	}
			    	reader.close();
					break;
				}
			}
			
			/*Path projection = new Path("blockIds/"+split.toString().substring(split.toString().lastIndexOf("/")+1).replace(':', '_').replace('+', '_'));
			FileSystem fs;
			TreeSet<String> set = new TreeSet<String>();
		    try {
		    	fs = FileSystem.get(context.getConfiguration());
		    	SequenceFile.Reader reader = new SequenceFile.Reader(fs, projection, context.getConfiguration());
		    	ImmutableBytesWritable key = new ImmutableBytesWritable();
		    	while(reader.next(key)){
		    		set.add(Bytes.toString(key.get()));
		    	}
		    	reader.close();

		    } catch (IOException e) {
		    	e.printStackTrace();
		    }
		    String t = context.getConfiguration().get("h2rdf.tableName");
		    IndexTranslator translator = new IndexTranslator(t+"_Index");
            index = translator.translate(set);
            set.clear();*/
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}


		public void map(LongWritable key, Text value, Context context) throws IOException {
			InputStream is = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));
			NxParser nxp = new NxParser(is);
			Node[] tr = nxp.next();
			Long v1 = index.get(tr[0].toN3());
			Long v2 = index.get(tr[1].toN3());
			Long v3 = index.get(tr[2].toN3());
			if(v1==null || v2==null || v3==null)
				System.out.println("not found translation");
			ByteTriple btr = new ByteTriple(v1, v2, v3);
        	try {
				context.write(new ImmutableBytesWritable(btr.getSPOByte()), val);
	        	context.write(new ImmutableBytesWritable(btr.getSOPByte()), val);
	        	context.write(new ImmutableBytesWritable(btr.getOPSByte()), val);
	        	context.write(new ImmutableBytesWritable(btr.getOSPByte()), val);
	        	context.write(new ImmutableBytesWritable(btr.getPOSByte()), val);
	        	context.write(new ImmutableBytesWritable(btr.getPSOByte()), val);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			value.clear();
		}
  	
   }

   
   
   
   
   private void loadHFiles()throws Exception {
	   	Configuration conf = HBaseConfiguration.create();
 	    conf.addResource("hbase-default.xml");
 	    conf.addResource("hbase-site.xml");
 	    HBaseAdmin hadmin = new HBaseAdmin(conf);
 		Path hfofDir= new Path("out/I");
 		FileSystem fs = hfofDir.getFileSystem(conf);
 	    //if (!fs.exists(hfofDir)) {
 	    //  throw new FileNotFoundException("HFileOutputFormat dir " +
 	    //      hfofDir + " not found");
 	    //}
 	   // FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
 	    //if (familyDirStatuses == null) {
 	    //  throw new FileNotFoundException("No families found in " + hfofDir);
 	    //}
 	    int length =0;
 	    byte[][] splits = new byte[18000][];
		Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(hfofDir));
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
				//System.out.println("out/I/"+hfile.getName()+" \t "+Bytes.toStringBinary(first));
		  		splits[length]=first.clone();
		  		length++;
		}
		byte[][] splits1 = new byte[length][];

 	    for (int i = 0; i < splits1.length; i++) {
 	    		splits1[i]=splits[i];
 		}
 	    Arrays.sort(splits1, Bytes.BYTES_COMPARATOR);
 	  
 		
 	    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
 	    
 		HColumnDescriptor family= new HColumnDescriptor("I");
		family.setCompressionType(Algorithm.SNAPPY);
 		desc.addFamily(family); 
 		family= new HColumnDescriptor("S");
		family.setCompressionType(Algorithm.SNAPPY);
 		desc.addFamily(family); 
 		family= new HColumnDescriptor("T");
		family.setCompressionType(Algorithm.SNAPPY);
 		desc.addFamily(family); 
 		//family= new HColumnDescriptor("C");
 		//desc.addFamily(family); 
 		//for (int i = 0; i < splits.length; i++) {
 		//	System.out.println(Bytes.toStringBinary(splits[i]));
 		//}
 		conf.setInt("zookeeper.session.timeout", 600000);
 		if(hadmin.tableExists(TABLE_NAME)){
 			//hadmin.disableTable(TABLE_NAME);
 			//hadmin.deleteTable(TABLE_NAME);
 		}
 		else{
 			hadmin.createTable(desc, splits1);
 		}
 		//hadmin.createTable(desc);
 		String[] args1 = new String[2];
 		args1[0]="out";
 		args1[1]=TABLE_NAME;
 		//args1[1]="new2";
 		
 		ToolRunner.run(new LoadIncrementalHFiles(HBaseConfiguration.create()), args1);
 	
 }
}
