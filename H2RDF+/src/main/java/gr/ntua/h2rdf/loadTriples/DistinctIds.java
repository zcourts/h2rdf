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
package gr.ntua.h2rdf.loadTriples;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jruby.util.Sprintf;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;


public class DistinctIds implements Tool{

	public static int bucket = 3000000;
	public long ReducerChunk = 536870912;
	public int numReducers;
	public static double samplingRate= new Double("0.001") , sampleChunk=33554432;
	private static final String TABLE_NAME = "Counters";
	
	public Job createSubmittableJob(String[] args) throws IOException, ClassNotFoundException{
		//io.compression.codecs
      Job job = new Job();
      
      job.setInputFormatClass(TextInputFormat.class);
      Configuration conf = new Configuration();
      Path blockProjection = new Path("blockIds/" );
      Path translations = new Path("translations/" );
      Path sample = new Path("sample/" );
      Path temp = new Path("temp/" );
      Path uniqueIds = new Path("uniqueIds/" );
      FileSystem fs;
      try {
    	  fs = FileSystem.get(conf);
          if (fs.exists(uniqueIds)) {
        	  fs.delete(uniqueIds,true);
          }
          if (fs.exists(translations)) {
        	  fs.delete(translations,true);
          }
          if (fs.exists(blockProjection)) {
        	  fs.delete(blockProjection,true);
          }
          if (fs.exists(sample)) {
        	  fs.delete(sample,true);
          }
          if (fs.exists(temp)) {
        	  fs.delete(temp,true);
          }
          
          FileOutputFormat.setOutputPath(job, uniqueIds);
          Path inp = new Path(args[0]);
    	  FileInputFormat.setInputPaths(job, inp);
    	  
    	  double type=1;
    	  double datasetSize=0;
          if(fs.isFile(inp)){
        	  datasetSize = fs.getFileStatus(inp).getLen();
          }
          else if(fs.isDirectory(inp)){
        	  FileStatus[] s = fs.
        			  listStatus(inp);
        	  for (int i = 0; i < s.length; i++) {
        		  if(s[i].getPath().getName().toString().endsWith(".gz"))
        			  type=27;
        		  if(s[i].getPath().getName().toString().endsWith(".snappy"))
        			  type=10;
        		  datasetSize += s[i].getLen();
        	  }
          }
          else{
        	  FileStatus[] s = fs.globStatus(inp);
        	  for (int i = 0; i < s.length; i++) {
        		  if(s[i].getPath().getName().toString().endsWith(".gz"))
        			  type=27;
        		  if(s[i].getPath().getName().toString().endsWith(".snappy"))
        			  type=10;
        		  datasetSize += s[i].getLen();
        	  }
          }
          datasetSize=datasetSize*type;
          System.out.println("type: "+type);
          System.out.println("datasetSize: "+datasetSize);
    	  samplingRate = (double)sampleChunk/(double)datasetSize;
    	  if(samplingRate>=0.1){
    		  samplingRate = 0.1;
    	  }
    	  if(samplingRate<=0.001){
    		  samplingRate = 0.001;
    	  }
    	  numReducers = (int) (datasetSize/ReducerChunk);
    	  if(numReducers==0)
    		  numReducers=1;
    	  numReducers++;
      } catch (IOException e) {
    	  e.printStackTrace();
      }
	  
	  HBaseAdmin hadmin = new HBaseAdmin(conf);
	  HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
	    
	  HColumnDescriptor family= new HColumnDescriptor("counter");
	  desc.addFamily(family); 
	  if(!hadmin.tableExists(TABLE_NAME)){
		  hadmin.createTable(desc);
	  }
	  

      job.setNumReduceTasks(numReducers);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass( ImmutableBytesWritable.class );
      job.setOutputValueClass( ImmutableBytesWritable.class );
      job.setOutputFormatClass( SequenceFileOutputFormat.class );
      job.setJarByClass(DistinctIds.class);
      job.setMapperClass(Map.class);
      job.setReducerClass( Reduce.class );

      job.setPartitionerClass(SamplingPartitioner.class);
      
      FileOutputFormat.setCompressOutput(job, true);
      FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
      job.getConfiguration().set("mapred.compress.map.output","true");
      job.getConfiguration().set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
      
      //job.setCombinerClass(Combiner.class);
      job.setJobName( "Distinct Id Wordcount" );
      job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
      job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
      job.getConfiguration().setInt("io.sort.mb", 100);
      job.getConfiguration().setInt("io.file.buffer.size", 131072);
      job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
      
      return job;
      
   }

   public int run(String[] args) throws Exception {
	    
		Job job =createSubmittableJob(args);
		job.waitForCompletion(true);
	    
		job = SortIds.createSubmittableJob(args, null, numReducers);
		job.waitForCompletion(true);
	    
		job = Translate.createSubmittableJob(args);
		job.waitForCompletion(true);

		SortIds.loadHFiles(args);
	    return 0;
   }
   
   public static class SamplingPartitioner<VALUE> extends Partitioner<ImmutableBytesWritable, VALUE>{

	@Override
	public int getPartition(ImmutableBytesWritable key, VALUE value, int numPartitions) {
		int v = ((IntWritable)value).get();
		if(v==0){
			return 0;
		}
		else{
			return ((key.hashCode() & Integer.MAX_VALUE) % (numPartitions-1))+1;
		}
	}
	   
   }
   
   
   public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, IntWritable> {
	   	public TreeMap<String,Integer> set;
		private Random r = new Random();
		private FSDataOutputStream sampleOut;
	   	NullWritable nullValue = NullWritable.get();
	   	private String name;
		private CompressionOutputStream cos;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			set = new TreeMap<String,Integer>();
			FileSplit split = (FileSplit) context.getInputSplit();
			name = split.toString().substring(split.toString().lastIndexOf("/")+1).replace(':', '_').replace('+', '_');
			
			
			Path sample = new Path("sample/"+name);
			FileSystem fs;
	    	fs = FileSystem.get(context.getConfiguration());
	        if (fs.exists(sample)) {
	        	fs.delete(sample,true);
	        }
        	sampleOut = fs.create(sample);
		    // Compress
        	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, context.getConfiguration());
    		cos = codec.createOutputStream(sampleOut);
    		    

			split.getPath();
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		    try {
		    	cos.close();
				sampleOut.flush();
				sampleOut.close();
				
				FileSystem fs;
		    	fs = FileSystem.get(context.getConfiguration());
		    	
				
				Path blockProjection = new Path("blockIds/"+name);
	
		        if (fs.exists(blockProjection)) {
		        	fs.delete(blockProjection,true);
		        }
	        	CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(GzipCodec.class, context.getConfiguration());
				SequenceFile.Writer projectionWriter = SequenceFile.createWriter(fs, context.getConfiguration(), blockProjection, 
						ImmutableBytesWritable.class, NullWritable.class,SequenceFile.CompressionType.BLOCK, codec);
				
				for(Entry<String, Integer> s : set.entrySet()){
					context.write(new ImmutableBytesWritable(Bytes.toBytes(s.getKey())), new IntWritable(s.getValue()));
					projectionWriter.append(new ImmutableBytesWritable(Bytes.toBytes(s.getKey())), nullValue);
					
					int rand=r.nextInt(1000000);
					if(rand<=1000000*samplingRate){
						try {
							context.write(new ImmutableBytesWritable(Bytes.toBytes(s.getKey())), new IntWritable(0));
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				projectionWriter.close();
				set.clear();
				
		    } catch (IOException e) {
		    	e.printStackTrace();
		    }
			
			super.cleanup(context);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
			InputStream is = new ByteArrayInputStream(value.toString().getBytes("UTF-8"));
			NxParser nxp = new NxParser(is);
			Node[] tr = nxp.next();
			
			/*Model model=ModelFactory.createDefaultModel();
			model.read(is, null, "N-TRIPLE");
            StmtIterator it = model.listStatements();
			Triple tr = it.nextStatement().asTriple();
			if(tr.getSubject().isBlank() || tr.getObject().isBlank()){
				String line = value.toString();
				String s,p,o;
				StringTokenizer tokenizer = new StringTokenizer(line);
				
				s=tokenizer.nextToken(" ");
				if(s.contains("\"")){
					if(!s.endsWith("\"") && !s.endsWith(">"))
						s+= tokenizer.nextToken("\"")+"\"";
				}
				p=tokenizer.nextToken(" ");
				if(p.contains("\"")){
					if(!p.endsWith("\"") && !p.endsWith(">"))
						p+= tokenizer.nextToken("\"")+"\"";
				}
				o=tokenizer.nextToken(" ");
				if(o.contains("\"")){
					if(!o.endsWith("\"") && !o.endsWith(">"))
						o+= tokenizer.nextToken("\"")+"\"";
				}
				
				if(tr.getSubject().isBlank() && tr.getObject().isBlank()){
					tr = new Triple(Node.createLiteral(s), tr.getPredicate(), Node.createLiteral(o));
				}
				else if(tr.getSubject().isBlank()){
					tr = new Triple(Node.createLiteral(s), tr.getPredicate(), tr.getObject());
				}
				else if(tr.getObject().isBlank()){
					tr = new Triple(tr.getSubject(), tr.getPredicate(), Node.createLiteral(o));
				}
			}*/
			
			//String s = tr.getSubject().toString();
			String s = tr[0].toN3();
        	Integer count=set.get(s);
        	if(count!=null){
        		count++;
        		set.put(s, count);
        	}
        	else{
        		set.put(s, 1);
        	}
        	String p = tr[1].toN3();
        	count=set.get(p);
        	if(count!=null){
        		count++;
        		set.put(p, count);
        	}
        	else{
        		set.put(p, 1);
        	}
        	String o = tr[2].toN3();
        	count=set.get(o);
        	if(count!=null){
        		count++;
        		set.put(o, count);
        	}
        	else{
        		set.put(o, 1);
        	}
			//System.out.println(tr[0].toN3()+" "+tr[1].toN3()+" "+tr[2].toN3()+" .");
			int rand=r.nextInt(1000000);
			if(rand<=1000000*samplingRate){

		        cos.write(Bytes.toBytes(tr[0].toN3()+" "+tr[1].toN3()+" "+tr[2].toN3()+" . \n"));
				//sampleOut.writeBytes(tr[0].toN3()+" "+tr[1].toN3()+" "+tr[2].toN3()+" . \n");
				//model.write(sampleOut, "N-TRIPLE");
				/*try {
					context.write(new ImmutableBytesWritable(Bytes.toBytes(tr[0].toN3())), new IntWritable(0));
					context.write(new ImmutableBytesWritable(Bytes.toBytes(tr[1].toN3())), new IntWritable(0));
					context.write(new ImmutableBytesWritable(Bytes.toBytes(tr[2].toN3())), new IntWritable(0));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
			}
			
			value.clear();
		}
  	
   }
   
   public static class Reduce extends Reducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

	   	public TreeSet<Pair> set;
	   	public int id, collected=0;
	   	public long chunks=0;
	   	public SequenceFile.Writer partitionWriter =null;
	   	public double chunkSize= bucket*samplingRate;
	   	NullWritable nullValue = NullWritable.get();
	   	HTable table;
	   	
	   	protected void setup(Context context) throws IOException,InterruptedException {
		   	super.setup(context);
			set = new TreeSet<Pair>();
			String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
			id = Integer.parseInt(idStr[idStr.length-2]);

			try {
				table = new HTable(HBaseConfiguration.create(), TABLE_NAME);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(id==0){
				Path stringIdPartition = new Path("partition/stringIdPartition");
				FileSystem fs;
			    try {
			    	fs = FileSystem.get(context.getConfiguration());
			    	if (fs.exists(stringIdPartition)) {
			        	fs.delete(stringIdPartition,true);
			        }
			    	partitionWriter = SequenceFile.createWriter(fs, context.getConfiguration(), stringIdPartition, ImmutableBytesWritable.class, NullWritable.class);
					
			    } catch (IOException e) {
			    	e.printStackTrace();
			    }
			    
			}
			else{
			}
	   	}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(id==0){
				System.out.println("chunks: "+chunks );
				Put put = new Put(Bytes.toBytes("count.chunks"));
				put.add(Bytes.toBytes("counter"), new byte[0], Bytes.toBytes(chunks+1));
				table.put(put);
				//context.getCounter("Countergroup", "count.chunks").setValue(chunks+1);
				partitionWriter.close();
			}
			else{
				long count=0;
				for (Pair entry : set) {
					try {
						SortedBytesVLongWritable value = new SortedBytesVLongWritable(count);
						/*byte[] b = new byte[8];
						byte[] idb = Bytes.toBytes(id-1);
						for (int i = 0; i < 4; i++) {
							b[i]=idb[i];
						}
	
						byte[] cb = Bytes.toBytes(count);
						for (int i = 0; i < 4; i++) {
							b[i+4]=cb[i];
						}*/
						context.write(entry.getValue(), new ImmutableBytesWritable(value.getBytesWithPrefix()));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					count++;
				}
				Put put = new Put(Bytes.toBytes("count."+id));
				put.add(Bytes.toBytes("counter"), new byte[0], Bytes.toBytes(count));
				table.put(put);
				//context.getCounter("Countergroup", "count."+id).setValue(count);
			}
			
			super.cleanup(context);
		}
	   
	   public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException {
		   int count = 0;
		   if(id==0){
			   collected++;
			   if( collected > chunkSize ) {
				   partitionWriter.append(new ImmutableBytesWritable(key.get()), nullValue);
				   collected=0;
	               chunks++;
	           }
		   }
		   else{
			   for(IntWritable value: values) {
				   count+=value.get();
			   }
			   Pair entry = new Pair(count, new ImmutableBytesWritable(key.get()));
			   set.add(entry);
		   }
		   
		   
		}



   }

   public static class Combiner extends Reducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable, IntWritable> {

		
	
	   public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException {
		   int count = 0;
		   for(IntWritable value: values) {
			   count+=value.get();
		   }
		   try {
			   context.write(key, new IntWritable(count));
		   } catch (InterruptedException e) {
			   e.printStackTrace();
		   }
		}
   }
   
   
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

}
