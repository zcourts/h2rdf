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
package sampler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.util.NodeFactory;

import byte_import.Combiner;
import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

public class TotalOrderPrep implements Tool{
	
   private static final float sumplingRate = new Float("0.01") ;
   private static final int bucketSampledTriples = 650000;//3000000(gia 1% sampling) //32MB regions
   public static long regions ;
   private static final String ARG_INPUTFORMAT="my.sample";
   private Configuration conf;

   public Job createSubmittableJob(String[] args) throws IOException, ClassNotFoundException{

      Job sample_job = new Job();

      // Remember the real input format so the sampling input format can use
      // it under the hood
      
      sample_job.getConfiguration().setBoolean( ARG_INPUTFORMAT, true);
      sample_job.setInputFormatClass(TextInputFormat.class);
      
      //sample_job.getConfiguration().set("mapred.fairscheduler.pool", "pool9");
      // Base the sample size on the number of reduce tasks that will be used
      // by the real job, but only use 1 reducer for this job (maps output very
      // little)
      sample_job.setNumReduceTasks(1);

      // Make this job's output a temporary filethe input file for the real job's
      // TotalOrderPartitioner
      Path partition = new Path("partitions/" );
      //partition.getFileSystem(job.getConfiguration()).deleteOnExit(partition);

      conf = new Configuration();
      FileSystem fs;
      try {
    	  fs = FileSystem.get(conf);
          if (fs.exists(partition)) {
        	  fs.delete(partition,true);
          }
      } catch (IOException e) {
    	  e.printStackTrace();
      }
      FileOutputFormat.setOutputPath(sample_job, partition);
	  FileInputFormat.setInputPaths(sample_job, new Path(args[0]));
      //TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(partition, "part-r-00000"));
      //job.setPartitionerClass(TotalOrderPartitioner.class);

      // If there's a combiner, turn it into an identity reducer to prevent
      // destruction of keys.
	  
	  
      sample_job.setCombinerClass(Combiner.class);

     
      sample_job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      sample_job.setMapOutputValueClass(ImmutableBytesWritable.class);
      sample_job.setOutputKeyClass( ImmutableBytesWritable.class );
      sample_job.setOutputValueClass( NullWritable.class );
      sample_job.setPartitionerClass( HashPartitioner.class );
      sample_job.setOutputFormatClass( SequenceFileOutputFormat.class );
      sample_job.setJarByClass(TotalOrderPrep.class);
      sample_job.setMapperClass(Map.class);
      sample_job.setReducerClass( PartitioningReducer.class );
      sample_job.setJobName( "(Sampler)" );
      sample_job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
      sample_job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
      return sample_job;
      
      // Run the job.  If it fails, then it's probably because of the main job.
      /*try {
         sample_job.waitForCompletion(false);

         if( !sample_job.isSuccessful() )
            throw new RuntimeException("Partition sampler job failed.");

      } catch (Exception e) {
         throw new RuntimeException("Failed to start Partition sampler.", e);
      }*/
   }

   public int run(String[] args) throws Exception {
	    
		Job job =createSubmittableJob(args);
		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		regions=counters.getGroup("org.apache.hadoop.mapred.Task$Counter").
				findCounter("REDUCE_OUTPUT_RECORDS").getValue()+1;
		
	    return 0;
   }
   
   
   public static class Map extends Mapper<LongWritable, Text, ImmutableBytesWritable, ImmutableBytesWritable> {
		private byte[] subject;
		private byte[] predicate;
		private byte[] object;
		private byte[] non;
		private ImmutableBytesWritable new_key = new ImmutableBytesWritable();
		private Random r = new Random();
		private static final int totsize=ByteValues.totalBytes;
		private static Boolean sampling ;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			sampling = context.getConfiguration().getBoolean(ARG_INPUTFORMAT,false);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
			if(sampling){
				int rand=r.nextInt(1000000);
				if(rand>1000000*sumplingRate)
					return;
			}
			
			non=Bytes.toBytes("");
			String line = value.toString();
			String s,p,o;
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			s=tokenizer.nextToken(" ");
			if(s.contains("\"")){
				if(!s.endsWith("\"") && !s.endsWith(">"))
					s+= tokenizer.nextToken("\"")+"\"";
			}
			subject=Bytes.toBytes(s);
			//System.out.println(subject);
			p=tokenizer.nextToken(" ");
			if(p.contains("\"")){
				if(!p.endsWith("\"") && !p.endsWith(">"))
					p+= tokenizer.nextToken("\"")+"\"";
			}
			predicate=Bytes.toBytes(p);
			//System.out.println(predicate);
			o=tokenizer.nextToken(" ");
			if(o.contains("\"")){
				if(!o.endsWith("\"") && !o.endsWith(">"))
					o+= tokenizer.nextToken("\"")+"\"";
			}
			object=Bytes.toBytes(o);
			//System.out.println(object);
			//tokenizer.nextToken();
			//if (tokenizer.hasMoreTokens()) {
			//	return ;
			//}
			/*try{
				//Reader r = new StringReader(value.toString());
				Node s1 = NodeFactory.parseNode(s);
				Node p1= NodeFactory.parseNode(p);
				Node o1 = NodeFactory.parseNode(o);
				//Triple t = new Triple(s1, p1, o1);
				//System.out.println(s1.toString(false));
				//System.out.println(p1.toString(false));
				//System.out.println(o1.toString(false));
				
				return ;
			}catch (Exception e) {
				System.out.println(value.toString());
				e.printStackTrace();
				return;
			}*/
			try {

				byte[] si = ByteValues.getFullValue(s);
		    	byte[] pi = ByteValues.getFullValue(p);
		    	byte[] oi = ByteValues.getFullValue(o);
		    	
		    	
		    	
		    	//dhmiourgia pinaka me indexes kanoume emit hashvalue-name, byte[0]=1
		    	byte[] k;
		    	if(si[0]==ByteValues.TYPE_STRING){
			    	k = new byte[subject.length+totsize+1];
					k[0] =	(byte)1;
			    	for (int i = 0; i < totsize; i++) {
						k[i+1]=si[i];
					}
			    	for (int i = 0; i < subject.length; i++) {
						k[i+totsize+1]=subject[i];
			    	}
					new_key.set(k, 0, k.length);
					context.write(new_key, new ImmutableBytesWritable(non,0,0));
		    	}
		    	if(pi[0]==ByteValues.TYPE_STRING){
					k = new byte[predicate.length+totsize+1];
					k[0] =	(byte)1;
			    	for (int i = 0; i < totsize; i++) {
						k[i+1]=pi[i];
					}
			    	for (int i = 0; i < predicate.length; i++) {
						k[i+totsize+1]=predicate[i];
			    	}
					new_key.set(k, 0, k.length);
					context.write(new_key, new ImmutableBytesWritable(non,0,0));
		    	}
		    	if(oi[0]==ByteValues.TYPE_STRING){
					k = new byte[object.length+totsize+1];
					k[0] =	(byte)1;
			    	for (int i = 0; i < totsize; i++) {
						k[i+1]=oi[i];
					}
			    	for (int i = 0; i < object.length; i++) {
						k[i+totsize+1]=object[i];
			    	}
					new_key.set(k, 0, k.length);
					context.write(new_key, new ImmutableBytesWritable(non,0,0));
		    	}
				
				//dhmiourgia spo byte[0]=4 emit key=si,pi value=oi
				k = new byte[totsize+totsize+totsize+1];
				k[0] =	(byte)4;
		    	for (int i = 0; i < totsize; i++) {
					k[i+1]=si[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+1]=pi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+totsize+1]=oi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				//dhmiourgia osp byte[0]=2 emit key=oi,si value=pi
				k = new byte[totsize+totsize+totsize+1];
				k[0] =	(byte)2;
		    	for (int i = 0; i < totsize; i++) {
					k[i+1]=oi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+1]=si[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+totsize+1]=pi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				//dhmiourgia pos byte[0]=3 emit key=pi,oi value=si
				k = new byte[totsize+totsize+totsize+1];
				k[0] =	(byte)3;
		    	for (int i = 0; i < totsize; i++) {
					k[i+1]=pi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+1]=oi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+totsize+1]=si[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				//dhmiourgia sop byte[0]=5 emit key=si,oi value=pi
				k = new byte[totsize+totsize+totsize+1];
				k[0] =	(byte)5;
		    	for (int i = 0; i < totsize; i++) {
					k[i+1]=si[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+1]=oi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+totsize+1]=pi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (NotSupportedDatatypeException e) {
				e.printStackTrace();
			}
		}
  	
 	}
   
   public static class MapNew extends Mapper<LongWritable, Text, ImmutableBytesWritable, ImmutableBytesWritable> {
		private byte[] subject;
		private byte[] predicate;
		private byte[] object;
		private byte[] non;
		private ImmutableBytesWritable new_key = new ImmutableBytesWritable();
		private Random r = new Random();
		private static final int totsize=ByteValues.totalBytes;
		private static Boolean sampling ;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			sampling = context.getConfiguration().getBoolean(ARG_INPUTFORMAT,false);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException {
			if(sampling){
				int rand=r.nextInt(1000000);
				if(rand>1000000*sumplingRate)
					return;
			}
			
			non=Bytes.toBytes("");
			String line = value.toString();
			String s,p,o;
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			s=tokenizer.nextToken(" ");
			if(s.contains("\"")){
				if(!s.endsWith("\"") && !s.endsWith(">"))
					s+= tokenizer.nextToken("\"")+"\"";
			}
			subject=Bytes.toBytes(s);
			//System.out.println(subject);
			p=tokenizer.nextToken(" ");
			if(p.contains("\"")){
				if(!p.endsWith("\"") && !p.endsWith(">"))
					p+= tokenizer.nextToken("\"")+"\"";
			}
			predicate=Bytes.toBytes(p);
			//System.out.println(predicate);
			o=tokenizer.nextToken(" ");
			if(o.contains("\"")){
				if(!o.endsWith("\"") && !o.endsWith(">"))
					o+= tokenizer.nextToken("\"")+"\"";
			}
			object=Bytes.toBytes(o);
			//System.out.println(object);
			//tokenizer.nextToken();
			//if (tokenizer.hasMoreTokens()) {
			//	return ;
			//}
			/*try{
				//Reader r = new StringReader(value.toString());
				Node s1 = NodeFactory.parseNode(s);
				Node p1= NodeFactory.parseNode(p);
				Node o1 = NodeFactory.parseNode(o);
				//Triple t = new Triple(s1, p1, o1);
				//System.out.println(s1.toString(false));
				//System.out.println(p1.toString(false));
				//System.out.println(o1.toString(false));
				
				return ;
			}catch (Exception e) {
				System.out.println(value.toString());
				e.printStackTrace();
				return;
			}*/
			try {

				byte[] si = ByteValues.getFullValue(s);
		    	byte[] pi = ByteValues.getFullValue(p);
		    	byte[] oi = ByteValues.getFullValue(o);
		    	byte[] tel = null, type=null;
				try {
					tel = ByteValues.getFullValue("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>");
					type = ByteValues.getFullValue("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
				} catch (NotSupportedDatatypeException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    	if(Bytes.equals(pi, tel) || Bytes.equals(pi, type))
		    		return;
		    	
		    	//dhmiourgia pinaka me indexes kanoume emit hashvalue-name, byte[0]=1
		    	byte[] k;
				
				//dhmiourgia spo byte[0]=4 emit key=si,pi value=oi
				k = new byte[totsize+totsize+totsize+1];
				k[totsize+totsize] = (byte)2;
		    	for (int i = 0; i < totsize; i++) {
					k[i]=si[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize]=oi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+totsize+1]=pi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));
				//dhmiourgia spo byte[0]=4 emit key=si,pi value=oi
				
				k = new byte[totsize+totsize+totsize+1];
				k[totsize+totsize] = (byte)3;
		    	for (int i = 0; i < totsize; i++) {
					k[i]=oi[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize]=si[i];
				}
		    	for (int i = 0; i < totsize; i++) {
					k[i+totsize+totsize+1]=pi[i];
				}
				new_key.set(k, 0, k.length);
				context.write(new_key, new ImmutableBytesWritable(non,0,0));

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (NotSupportedDatatypeException e) {
				e.printStackTrace();
			}
		}
 	
	}
   /**
    * This reducer only emits enough keys to fill the partition file.
    */
   public static class PartitioningReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, NullWritable> {

      public void run(Context context) throws IOException, InterruptedException {
    	  
    	  int collected=0, chunks=0;
    	  float chunkSize= bucketSampledTriples*sumplingRate;
    	  System.out.println("chunkSize: "+chunkSize);
    	  while( context.nextKey() ) {
              if( collected > chunkSize ) {
                 context.write(context.getCurrentKey(), NullWritable.get());
                 collected=0;
                 chunks++;
              }
              else{
            	  collected++;
              }
           }
    	  System.out.println("chunks: "+chunks);
      }

   }

	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
