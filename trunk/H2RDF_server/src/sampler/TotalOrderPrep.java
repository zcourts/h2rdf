package sampler;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;

import byte_import.Combiner;
import byte_import.HexastoreBulkImport;
import byte_import.HexastoreBulkImport.Map;

public class TotalOrderPrep implements Tool{
   private static final String ARG_SAMPLESIZE = "top.sample.size";

   public Job createSubmittableJob(String[] args) throws IOException, ClassNotFoundException{

      Job sample_job = new Job();

      // If there's only one reduce task, don't bother with total order
      // partitioning.
      /*if( job.getNumReduceTasks() == 1 ) {
         job.setPartitionerClass( HashPartitioner.class );
         return;
      }*/

      // Remember the real input format so the sampling input format can use
      // it under the hood
      //sample_job.getConfiguration().setClass( ARG_INPUTFORMAT, TextInputFormat.class, InputFormat.class);
      sample_job.setInputFormatClass(TextInputFormat.class);

      // Base the sample size on the number of reduce tasks that will be used
      // by the real job, but only use 1 reducer for this job (maps output very
      // little)
      sample_job.getConfiguration().setInt(ARG_SAMPLESIZE, HexastoreBulkImport.regions);
      sample_job.setNumReduceTasks(1);

      // Make this job's output a temporary filethe input file for the real job's
      // TotalOrderPartitioner
      Path partition = new Path("/user/npapa/"+HexastoreBulkImport.regions+"partitions/" );
      //partition.getFileSystem(job.getConfiguration()).deleteOnExit(partition);

      FileOutputFormat.setOutputPath(sample_job, partition);
	  FileInputFormat.setInputPaths(sample_job, new Path("/user/npapa/LUBMsample/M*.txt"));
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
      sample_job.setJarByClass(HexastoreBulkImport.class);
      sample_job.setMapperClass(Map.class);
      sample_job.setReducerClass( PartitioningReducer.class );
      sample_job.setJobName( "(Sampler)" );
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
	    return 0;
   }
   
   /**
    * This reducer only emits enough keys to fill the partition file.
    */
   public static class PartitioningReducer extends Reducer {

      public void run(Context context) throws IOException, InterruptedException {
         // I wish there was a better way to get this, but alas Task.Counter
         // is protected...
         long N = context.getCounter("org.apache.hadoop.mapred.Task$Counter",
                                    "MAP_OUTPUT_RECORDS").getValue();
         long K = context.getConfiguration().getInt(ARG_SAMPLESIZE, 1);

         // Set the collection rate so that it will collects more frequently
         // than ideal.  It makes the last partition very slightly larger,
         // but avoids corner cases caused by round off errors, and keeps
         // the reducer from having to scan keys in the last partition.
         
         N=329437;//1
         N=5041520;//10
         N=43910101;//100
         
         
         double collect_rate = Math.max(1.0, (N-1) / (double)K);
         System.out.println("N: "+N);
         System.out.println("K: "+K);
         System.out.println("collect_rate: "+collect_rate);
         //System.exit(1);
         double emit = 0.0;
         int collected = 0;

         while( collected < K-1 && context.nextKey() ) {
        	Iterator it = context.getValues().iterator();
        	/*while(it.hasNext()){
        		it.next();
                emit += 1.0;
        	}*/
            emit += 1.0;
            if( emit > collect_rate ) {
               context.write(context.getCurrentKey(), NullWritable.get());
               emit -= collect_rate;
               ++collected;
            }
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
