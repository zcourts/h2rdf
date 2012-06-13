/*******************************************************************************
 * Copyright (c) 2012 Nikos Papailiou. 
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Nikos Papailiou - initial API and implementation
 ******************************************************************************/
package byte_import;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import bytes.ByteValues;

import sampler.TotalOrderPartitioner;
import sampler.TotalOrderPrep;

public class HexastoreBulkImport implements Tool {
  private static final String NAME = "Load rdf triples";
  private static String TABLE_NAME;
  private Configuration conf;

  
  
  public static class Reduce extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
	  
	  	private static byte[] prevRowKey = null;
	  	private static Long rowSize;
	  	private static byte[] prev2varKey = null;
	  	private static Long row2varSize;
	  	private static int regionSize, regionId, first;
	  	private static int MAX_REGION=1000000; //max number of column per row
	    private static final long Statistics_offset=new Long(50); 
		private static final int totsize=ByteValues.totalBytes;
		private static int max_regions;
		private static HTable statsTable;
	  	
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException {
			
			String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
			//byte[] id =Bytes.toBytes(Short.parseShort(idStr[idStr.length-2]));
			
			byte[] k = key.get();
			byte pin = (byte) k[0];
			
			
			if(pin==(byte) 1){
				byte[] k1 = new byte[totsize+1];
				for (int i = 0; i < k1.length; i++) {
					k1[i]=k[i];
				}
				byte[] k2 = new byte[k.length-totsize-1];
				for (int i = 0; i < k2.length; i++) {
					k2[i]=k[i+totsize+1];
				}
				ImmutableBytesWritable emmitedKey = new ImmutableBytesWritable(
						k1, 0, k1.length);
				
	
				KeyValue emmitedValue = new KeyValue(emmitedKey.get(), Bytes
						.toBytes("A"), Bytes
						.toBytes("i"), k2);
	
			    try {
			    		context.write(emmitedKey, emmitedValue);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else{
				if(k.length!=1+3*totsize){
					System.exit(1);
				}
				byte[] newKey= new byte[1+2*totsize+2];
				for (int i = 0; i < newKey.length-2; i++) {
					newKey[i]=k[i];
				}
				
				
				
				if(prevRowKey==null){
					regionSize=0;
					regionId=0;
					rowSize=new Long(0);
					prevRowKey = new byte[1+2*totsize];
					for (int i = 0; i < prevRowKey.length; i++) {
						prevRowKey[i]=newKey[i];
					}
				}
				
				if(first==0 /*&& newKey[0]==(byte)3*/){
					first++;
					row2varSize=new Long(0);
					prev2varKey = new byte[1+totsize];
					regionSize=0;
					regionId=0;
					rowSize=new Long(0);
					prevRowKey = new byte[1+2*totsize];
					for (int i = 0; i < prevRowKey.length; i++) {
						prevRowKey[i]=newKey[i];
					}
					for (int i = 0; i < prev2varKey.length; i++) {
						prev2varKey[i]=newKey[i];
					}
				}
				
				if(rowChanged(newKey)){//allazei grammh 
					//statistics
					row2varSize+=rowSize;
					if(rowSize>=Statistics_offset)
						addStatistics(prevRowKey, rowSize);
				
					if(row2varChanged(newKey)){//allazei to prwto id ths grammhs
						if(row2varSize>=Statistics_offset)
							addStatistics(prev2varKey, row2varSize);
						
						for (int i = 0; i < prev2varKey.length; i++) {
							prev2varKey[i]=newKey[i];
						}
						row2varSize=new Long(0);
					}
					//else{//idia grammh auksanw to size
					//	row2varSize+=rowSize;
					//}
					//statistics
					row2varSize++;
					
					for (int i = 0; i < prevRowKey.length; i++) {
						prevRowKey[i]=newKey[i];
					}
					rowSize=new Long(1);
					regionSize=1;
					regionId=0;
				}
				else{//idia grammh auksanw to size
					regionSize++;
					rowSize++;
					if(regionSize > MAX_REGION){
						if(regionId<=max_regions-2){
							regionId++;
							regionSize=1;
						}
					}
				}

				byte[] tid = Bytes.toBytes((short)(max_regions*
						Integer.parseInt(idStr[idStr.length-2])+regionId));
				newKey[newKey.length-2]=tid[0];
				newKey[newKey.length-1]=tid[1];

				/*if(regionId<max_regions){
					byte[] tid = Bytes.toBytes((short)(max_regions*
							Integer.parseInt(idStr[idStr.length-2])+regionId));
					newKey[newKey.length-2]=tid[0];
					newKey[newKey.length-1]=tid[1];
				}
				else{
					System.out.println("Too many regions");
					byte[] tid = Bytes.toBytes(Short.MAX_VALUE);
					newKey[newKey.length-2]=tid[0];
					newKey[newKey.length-1]=tid[1];
				}*/
				
				
				ImmutableBytesWritable emmitedKey = new ImmutableBytesWritable(
						newKey, 0, newKey.length);
				byte[] val = new byte[totsize];
				for (int i = 0; i < val.length; i++) {
					val[i]=k[1+2*totsize+i];
				}
				KeyValue emmitedValue = new KeyValue(emmitedKey.get().clone(), Bytes
						.toBytes("A"), val.clone(), null);
				try {
			    	context.write(emmitedKey, emmitedValue);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			
			}
		    
		}

		private void addStatistics(byte[] row, long size){
			try {
				statsTable.incrementColumnValue(row, Bytes.toBytes("size"), Bytes.toBytes(""), size);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		private boolean row2varChanged(byte[] newKey) {
			boolean changed=false;
			for (int i = 0; i < prev2varKey.length; i++) {
				if(prev2varKey[i]!=newKey[i]){
					changed=true;
					break;
				}
			}
			return changed;
		}

		private boolean rowChanged(byte[] newKey) {
			boolean changed=false;
			for (int i = 0; i < prevRowKey.length; i++) {
				if(prevRowKey[i]!=newKey[i]){
					changed=true;
					break;
				}
			}
			return changed;
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			prevRowKey = null;
		  	rowSize=new Long(0);
		  	prev2varKey = null;
		  	row2varSize= new Long(0);
		  	regionSize=0;
		  	regionId=0;
		  	first=0;
			String t = context.getConfiguration().get("h2rdf.tableName");
		  	statsTable=new HTable(t+"_stats" );
			int reducersNum=Integer.parseInt(context.getConfiguration().get("mapred.reduce.tasks"));
			max_regions=Short.MAX_VALUE/reducersNum;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(rowSize>=Statistics_offset)
				addStatistics(prevRowKey, rowSize);
			if(row2varSize>=Statistics_offset)
				addStatistics(prev2varKey, row2varSize);
			super.cleanup(context);
		}
	}
  
  
  public Job createSubmittableJob(String[] args) {
	  TABLE_NAME=args[1];
    Job job = null;
	try {
		job = new Job(new Configuration(), NAME);
		job.setJarByClass(HexastoreBulkImport.class);
		job.setMapperClass(sampler.TotalOrderPrep.Map.class);
		job.setReducerClass(Reducer.class);
		job.setCombinerClass(Combiner.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(ImmutableBytesWritable.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		//TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("/user/npapa/"+regions+"partitions/part-r-00000"));
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("partitions/part-r-00000"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		Path out=new Path("out");
		FileOutputFormat.setOutputPath(job, out);
		Configuration conf = new Configuration();
	    FileSystem fs;
	    try {
	    	fs = FileSystem.get(conf);
	        if (fs.exists(out)) {
	        	fs.delete(out,true);
	        }
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	    
	    HBaseAdmin hadmin = new HBaseAdmin(conf);
	    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME+"_stats");
		HColumnDescriptor family= new HColumnDescriptor("size");
		desc.addFamily(family); 
		conf.setInt("zookeeper.session.timeout", 600000);
		if(hadmin.tableExists(TABLE_NAME+"_stats")){
			//hadmin.disableTable(TABLE_NAME+"_stats");
			//hadmin.deleteTable(TABLE_NAME+"_stats");
		}
		else{
			hadmin.createTable(desc);
		}
		
	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    //job.getConfiguration().setInt("mapred.map.tasks", 18);
		job.getConfiguration().set("h2rdf.tableName", TABLE_NAME);
		job.getConfiguration().setInt("mapred.reduce.tasks", (int) TotalOrderPrep.regions);
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
		job.getConfiguration().setInt("io.sort.mb", 100);
		job.getConfiguration().setInt("io.file.buffer.size", 131072);
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
		//job.getConfiguration().setInt("hbase.hregion.max.filesize", 67108864);
		job.getConfiguration().setInt("hbase.hregion.max.filesize", 33554432);
		job.getConfiguration().setInt("mapred.tasktracker.map.tasks.maximum", 5);
	    job.getConfiguration().setInt("mapred.tasktracker.reduce.tasks.maximum", 5);
		//job.getConfiguration().setInt("io.sort.mb", 100);
		
	} catch (IOException e2) {
		e2.printStackTrace();
	}
    
    return job;
  }

  
  public int run(String[] args) throws Exception {
    
	Job job =createSubmittableJob(args);
	job.waitForCompletion(true);
	loadHFiles();
    return 0;
  }

  private void loadHFiles()throws Exception {
		conf = HBaseConfiguration.create();
	  HBaseAdmin hadmin = new HBaseAdmin(conf);
		Path hfofDir= new Path("out");
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
		
	    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
	    
		HColumnDescriptor family= new HColumnDescriptor("A");
		desc.addFamily(family); 
		//for (int i = 0; i < splits.length; i++) {
		//	System.out.println(Bytes.toStringBinary(splits[i]));
		//}
		conf.setInt("zookeeper.session.timeout", 600000);
		if(hadmin.tableExists(TABLE_NAME)){
			hadmin.disableTable(TABLE_NAME);
			hadmin.deleteTable(TABLE_NAME);
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


public Configuration getConf() {
    return this.conf;
  } 

  public void setConf(final Configuration c) {
    this.conf = c;
  }



}
