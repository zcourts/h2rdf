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
package translator;


import input_format.FileTableInputFormat;
import input_format.MyFileInputFormat;
import input_format.TableInputFormat;
import input_format.TableMapReduceUtil;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

import partialJoin.*;
import byte_import.HexastoreBulkImport;
import byte_import.MyNewTotalOrderPartitioner;

public class MRTranslate2 implements Tool{

	private Configuration conf;
    public int run(String[] args) throws Exception {
    	
		Job job = new Job(JoinPlaner.joinConf, "Translate2");
		job.setJarByClass(MRTranslate2.class);
		job.setMapperClass(MRTranslateMapper2.class);
		job.setReducerClass(MRTranslateReducer2.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		//job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	    //job.setMapOutputValueClass(Text.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	FileInputFormat.addInputPath(job, new Path(args[0]));
	    
		FileSystem fs = FileSystem.get(conf);
			
		//job.getConfiguration().setInt("mapred.map.tasks", 18);
		String p=JoinPlaner.joinConf.get("mapred.fairscheduler.pool");
		int  max_tasks = Integer.parseInt(p.substring(p.indexOf("l")+1));
		job.getConfiguration().setInt("mapred.reduce.tasks", max_tasks);
		job.getConfiguration().setBoolean(
				"mapred.map.tasks.speculative.execution", false);
		job.getConfiguration().setBoolean(
				"mapred.reduce.tasks.speculative.execution", false);
		job.getConfiguration().setInt("io.sort.mb", 100);
		job.getConfiguration().setInt("io.file.buffer.size", 131072);
		job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);
		
		//job.getConfiguration().setInt("io.sort.mb", 100);
		    
		
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

