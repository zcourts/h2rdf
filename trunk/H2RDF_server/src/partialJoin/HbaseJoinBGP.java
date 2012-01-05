package partialJoin;

import input_format.FileTableInputFormat;
import input_format.MyFileInputFormat;
import input_format.TableInputFormat;
import input_format.TableMapReduceUtil;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

import byte_import.MyNewTotalOrderPartitioner;

public class HbaseJoinBGP implements Tool{

	private Configuration conf;
    public int run(String[] args) throws Exception {
		Job job = new Job(getConf(), "Join");
		job.setJarByClass(HbaseJoinBGP.class);
		job.setMapperClass(HbaseJoinBGPMapper.class);
		job.setReducerClass(HbaseJoinBGPReducer.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		//job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    //job.setMapOutputValueClass(Text.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("nikos.inputfile", "input/JoinVars_"+JoinPlaner.id+"_"+(JoinPlaner.joins-1));
		job.getConfiguration().set("nikos.table", JoinPlaner.getTable());

	    FileOutputFormat.setOutputPath(job, new Path("output/"+args[0]));
	    String[] p= new String[args.length];
	    int pi=0;
	    for (int i = 1; i < args.length; i++) {
	    	int k=0;
	    	for (int j = 0; j < pi; j++) {
		    	if(args[i].equals(p[j])){
		    		k++;
		    	}
		    }
	    	
	    	if(k==0){
	    		p[pi]=args[i];
	    		pi++;
	    	}
	    }
		TableMapReduceUtil.newJob();
		int file=0,type=0;
		for (int i = 0; i < pi; i++) {
		    System.out.println(p[i]+"jjjjjjjjjjjjjjj");
		    if(p[i].contains("BGP")){
		    	int no=Integer.parseInt(p[i].substring(p[i].length()-1));
			    Scan scan =JoinPlaner.getScan(no);
			    String col=scan.getInputColumns();
			    byte[] startRow =scan.getStartRow();
			    byte[] stopRow =scan.getStopRow();
			    if(col.contains("?")){
			    	TableMapReduceUtil.addRow(JoinPlaner.getinpVars(no), "P"+no, JoinPlaner.getTable(), startRow, stopRow, "A:", job);
			    }
			    else{
			    	TableMapReduceUtil.addCol(JoinPlaner.getinpVars(no), "P"+no, JoinPlaner.getTable(), startRow, stopRow, col, job);
			    }
		    }
		    else{
		    	file++;
		    	MyFileInputFormat.addInputPath(job, new Path(p[i]));
		    	type++;
		    }
		}
			
		//job.getConfiguration().setInt("mapred.map.tasks", 18);
		//job.getConfiguration().setInt("mapred.reduce.tasks", 25);
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
		job.getConfiguration().setInt("io.sort.mb", 100);
		job.getConfiguration().setInt("io.file.buffer.size", 131072);
		/*job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
		
		job.getConfiguration().setInt("io.sort.mb", 100);*/
		if(file==0)
			job.setInputFormatClass(TableInputFormat.class);
		else
		    job.setInputFormatClass(FileTableInputFormat.class);
		
	    job.waitForCompletion(true);
	    return 0;
    }

    public Configuration getConf() {
    	return this.conf;
    } 

  	public void setConf(final Configuration c) {
        this.conf = c;
  	}
}
