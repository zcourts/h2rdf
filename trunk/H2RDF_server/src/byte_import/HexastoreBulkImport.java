package byte_import;


import java.io.IOException;
import java.lang.reflect.Array;
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
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import input_format.MultiHFileOutputFormat;
import sampler.TotalOrderPartitioner;

public class HexastoreBulkImport implements Tool {
  private static final String NAME = "Byte_Store";
  private Configuration conf;

  public static int MAX_TASKS=50, regions=1000;
  
  public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, ImmutableBytesWritable> {
		private byte[] subject;
		private byte[] predicate;
		private byte[] object;
		private byte[] non;
		private ImmutableBytesWritable new_key = new ImmutableBytesWritable();
	    private static MD5Hash md5h;
	    
		public void map(LongWritable key, Text value, Context context) throws IOException {
			
			non=Bytes.toBytes("");
			String line = value.toString();
			String s,p,o;
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			s=tokenizer.nextToken(" ");
			if(s.contains("\"")){
				if(!s.endsWith("\""))
					s+= tokenizer.nextToken("\"")+"\"";
			}
			subject=Bytes.toBytes(s);
			//System.out.println(subject);
			p=tokenizer.nextToken(" ");
			if(p.contains("\"")){
				if(!p.endsWith("\""))
					p+= tokenizer.nextToken("\"")+"\"";
			}
			predicate=Bytes.toBytes(p);
			//System.out.println(predicate);
			o=tokenizer.nextToken(" ");
			if(o.contains("\"")){
				if(!o.endsWith("\""))
					o+= tokenizer.nextToken("\"")+"\"";
			}
			object=Bytes.toBytes(o);
			//System.out.println(object);
			//tokenizer.nextToken();
			//if (tokenizer.hasMoreTokens()) {
			//	return ;
			//}
			
			
			try {

		    	byte[] si = getHash(s);
		    	byte[] pi = getHash(p);
		    	byte[] oi = getHash(o);
		    	//dhmiourgia pinaka me indexes kanoume emit hashvalue-name, byte[0]=1
		    	byte[] k = new byte[subject.length+8+1];
				k[0] =	(byte)1;
		    	for (int i = 0; i < 8; i++) {
					k[i+1]=si[i];
				}
		    	for (int i = 0; i < subject.length; i++) {
					k[i+8+1]=subject[i];
		    	}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));

				k = new byte[predicate.length+8+1];
				k[0] =	(byte)1;
		    	for (int i = 0; i < 8; i++) {
					k[i+1]=pi[i];
				}
		    	for (int i = 0; i < predicate.length; i++) {
					k[i+8+1]=predicate[i];
		    	}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				
				k = new byte[object.length+8+1];
				k[0] =	(byte)1;
		    	for (int i = 0; i < 8; i++) {
					k[i+1]=oi[i];
				}
		    	for (int i = 0; i < object.length; i++) {
					k[i+8+1]=object[i];
		    	}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				
				//dhmiourgia spo byte[0]=4 emit key=si,pi value=oi
				k = new byte[8+8+8+1];
				k[0] =	(byte)4;
		    	for (int i = 0; i < 8; i++) {
					k[i+1]=si[i];
				}
		    	for (int i = 0; i < 8; i++) {
					k[i+8+1]=pi[i];
				}
		    	for (int i = 0; i < 8; i++) {
					k[i+8+8+1]=oi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				//dhmiourgia osp byte[0]=2 emit key=oi,si value=pi
				k = new byte[8+8+8+1];
				k[0] =	(byte)2;
		    	for (int i = 0; i < 8; i++) {
					k[i+1]=oi[i];
				}
		    	for (int i = 0; i < 8; i++) {
					k[i+8+1]=si[i];
				}
		    	for (int i = 0; i < 8; i++) {
					k[i+8+8+1]=pi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				//dhmiourgia pos byte[0]=3 emit key=pi,oi value=si
				k = new byte[8+8+8+1];
				k[0] =	(byte)3;
		    	for (int i = 0; i < 8; i++) {
					k[i+1]=pi[i];
				}
		    	for (int i = 0; i < 8; i++) {
					k[i+8+1]=oi[i];
				}
		    	for (int i = 0; i < 8; i++) {
					k[i+8+8+1]=si[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		
		private byte[] getHash(String string) {
			
			md5h = MD5Hash.digest(string);
			long hashVal = Math.abs(md5h.halfDigest());
			
			byte[] b = Bytes.toBytes(hashVal);
			if (b.length<8){
				System.exit(5);
			}
			else if (b.length>8){
				System.exit(6);
			}
			
			return b;
		}
   
  	}
  
  
  public static class Reduce extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
	  
	  	private static byte[] prevRowKey = null;
	  	private static Long rowSize;
	  	private static byte[] prev2varKey = null;
	  	private static Long row2varSize;
	  	private static int regionSize, regionId, first;
	  	private static int MAX_REGION=1000000;
	  	
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException {
			
			String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
			byte[] id =Bytes.toBytes(Short.parseShort(idStr[idStr.length-2]));
			int reducersNum=Integer.parseInt(context.getConfiguration().get("mapred.reduce.tasks"));
			
			byte[] k = key.get();
			byte pin = (byte) k[0];
			
			
			if(pin==(byte) 1){
				byte[] k1 = new byte[8+1];
				for (int i = 0; i < k1.length; i++) {
					k1[i]=k[i];
				}
				byte[] k2 = new byte[k.length-9];
				for (int i = 0; i < k2.length; i++) {
					k2[i]=k[i+9];
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
				if(k.length!=25){
					System.exit(1);
				}
				byte[] newKey= new byte[19];
				for (int i = 0; i < newKey.length-2; i++) {
					newKey[i]=k[i];
				}
				
				
				/*if(prevRowKey==null){
					regionSize=0;
					regionId=0;
					rowSize=new Long(0);
					prevRowKey = new byte[17];
					for (int i = 0; i < prevRowKey.length; i++) {
						prevRowKey[i]=newKey[i];
					}
				}
				
				if(first==0 && newKey[0]==(byte)3){
					first++;
					row2varSize=new Long(0);
					prev2varKey = new byte[9];
					regionSize=0;
					regionId=0;
					rowSize=new Long(0);
					prevRowKey = new byte[17];
					for (int i = 0; i < prevRowKey.length; i++) {
						prevRowKey[i]=newKey[i];
					}
					for (int i = 0; i < prev2varKey.length; i++) {
						prev2varKey[i]=newKey[i];
					}
				}
				
				if(rowChanged(newKey)){//allazei grammh grafw to size me offset 255
					
					byte[] col = new byte[8];
					//statistics
					if(newKey[0]==(byte)3 ){
						byte[] newKey2= new byte[19];
						for (int i = 0; i < prevRowKey.length; i++) {
							newKey2[i]=prevRowKey[i];
						}
						newKey2[newKey2.length-2]=(byte) 255;//size table
						newKey2[newKey2.length-1]=(byte) 255;//size table
						ImmutableBytesWritable emmitedKey1 = new ImmutableBytesWritable(
								newKey2, 0, newKey2.length);
						
						for (int i = 0; i < 8; i++) {
							col[i]=Bytes.toBytes(rowSize)[i];
						}
						KeyValue emmitedValue1 = new KeyValue(emmitedKey1.get().clone(), Bytes
								.toBytes("A"), col.clone(), null);
						try {
							if(rowSize>=50)
								context.write(emmitedKey1, emmitedValue1);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						if(row2varChanged(newKey)){//allazei to prwto id ths grammhs grafw to size me offset 255
							byte[] newKey3= new byte[19];
							//row2varSize+=rowSize;
							for (int i = 0; i < prev2varKey.length; i++) {
								newKey3[i]=prev2varKey[i];
							}
							for (int i = prev2varKey.length; i < newKey3.length; i++) {
								newKey3[i]=(byte) 255;
							}
							
							//System.out.println("old_key2: "+ Bytes.toStringBinary(newKey2));
							//System.out.println("old_key3: "+ Bytes.toStringBinary(newKey3));
							//System.out.println("new_key: "+ Bytes.toStringBinary(newKey));
							
							ImmutableBytesWritable emmitedKey2 = new ImmutableBytesWritable(
									newKey3, 0, newKey3.length);
							for (int i = 0; i < 8; i++) {
								col[i]=Bytes.toBytes(row2varSize)[i];
							}
							KeyValue emmitedValue2 = new KeyValue(emmitedKey2.get().clone(), Bytes
									.toBytes("A"), col.clone(), null);
							try {
								if(row2varSize>=50)
									context.write(emmitedKey2, emmitedValue2);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							
							for (int i = 0; i < prev2varKey.length; i++) {
								prev2varKey[i]=newKey[i];
							}
							row2varSize=new Long(0);
						}
						else{//idia grammh auksanw to size
							row2varSize+=rowSize;
						}
					}
					//statistics
					
					
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
						regionId++;
						regionSize=1;
					}
				}
				int max_regions=20;
				if(regionId<=max_regions){
					byte[] tid = Bytes.toBytes((short)(max_regions*
							Integer.parseInt(idStr[idStr.length-2])+regionId));
					newKey[newKey.length-2]=tid[0];
					newKey[newKey.length-1]=tid[1];
				}
				else{
					System.out.println("Too many regions");
					System.exit(15);
				}*/
				
				
				
				//works
				if(id.length==2){
					for (int i = 0; i < 2; i++) {
						newKey[newKey.length-2+i]=id[i];
					}
				}
				else{
					System.exit(15);
				}
				
				ImmutableBytesWritable emmitedKey = new ImmutableBytesWritable(
						newKey, 0, newKey.length);
				byte[] val = new byte[8];
				for (int i = 0; i < val.length; i++) {
					val[i]=k[17+i];
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
		}
	}
  
  
  public Job createSubmittableJob(String[] args) {
	  
    Job job = null;
	try {
		job = new Job(new Configuration(), NAME);
		job.setJarByClass(HexastoreBulkImport.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(ImmutableBytesWritable.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("/user/npapa/"+regions+"partitions/part-r-00000"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("out"));
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    //job.getConfiguration().setInt("mapred.map.tasks", 18);
		job.getConfiguration().setInt("mapred.reduce.tasks", regions);
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
		job.getConfiguration().setInt("io.sort.mb", 100);
		job.getConfiguration().setInt("io.file.buffer.size", 131072);
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
		//job.getConfiguration().setInt("hbase.hregion.max.filesize", 67108864);
		job.getConfiguration().setInt("hbase.hregion.max.filesize", 33554432);
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
		
	  HBaseAdmin hadmin = new HBaseAdmin(conf);
		Path hfofDir= new Path("/user/npapa/out");
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
	    byte[][] splits = new byte[8000][];
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

		    HFile.Reader hfr = new HFile.Reader(fs, hfile, null, false);
		    final byte[] last,first;
		    try {
		      hfr.loadFileInfo();
		      first = hfr.getFirstRowKey();
		      last = hfr.getLastRowKey();
		    }  finally {
		      hfr.close();
		    }
		    splits[length]=first.clone();
		    length++;
	      }
	    }
	    System.out.println(length);
	    byte[][] splits1 = new byte[length][];
	    for (int i = 0; i < splits1.length; i++) {
	    	splits1[i]=splits[i];
		}
	    Arrays.sort(splits1, Bytes.BYTES_COMPARATOR);
		HTableDescriptor desc = new HTableDescriptor("new2");
		HColumnDescriptor family= new HColumnDescriptor("A");
		desc.addFamily(family); 
		//for (int i = 0; i < splits.length; i++) {
		//	System.out.println(Bytes.toStringBinary(splits[i]));
		//}
		/*if(hadmin.tableExists("new2")){
			hadmin.disableTable("new2");
			hadmin.deleteTable("new2");
		}*/
		conf.setInt("zookeeper.session.timeout", 600000);
		
		hadmin.createTable(desc, splits1);
		
		//hadmin.createTable(desc);
		String[] args1 = new String[2];
		args1[0]="out";
		args1[1]="new2";
		
		ToolRunner.run(new LoadIncrementalHFiles(HBaseConfiguration.create()), args1);
	
}


public Configuration getConf() {
    return this.conf;
  } 

  public void setConf(final Configuration c) {
    this.conf = c;
  }

}