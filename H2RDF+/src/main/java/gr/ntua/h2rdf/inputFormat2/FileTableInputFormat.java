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
package gr.ntua.h2rdf.inputFormat2;

import gr.ntua.h2rdf.indexScans.Bindings;
import gr.ntua.h2rdf.indexScans.JoinBGPMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.velocity.app.event.ReferenceInsertionEventHandler.referenceInsertExecutor;

public class FileTableInputFormat extends InputFormat implements Configurable {
	private static SequenceFileAsBinaryInputFormat fileInputFormat = new SequenceFileAsBinaryInputFormat();
	private static MultiTableInputFormat tableInputFormat = new MultiTableInputFormat();
	public static void initTableMapperJob(List<Scan> scans,
		      Class<? extends Mapper> mapper,
		      Class<? extends WritableComparable> outputKeyClass,
		      Class<? extends Writable> outputValueClass, Job job) throws IOException {
		job.getConfiguration().setBoolean("table", true);
	    TableMapReduceUtil.initTableMapperJob(scans, mapper
	    		, outputKeyClass, outputValueClass, job);
	}
	

	private Configuration conf;
	
	@Override
	public RecordReader createRecordReader(InputSplit split	,
			TaskAttemptContext context) throws IOException, InterruptedException {
		if(split.getClass().equals(TableSplit.class)){
			return tableInputFormat.createRecordReader(split, context);
		}
		else{
			return fileInputFormat.createRecordReader(split, context);
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		if(conf.getBoolean("table",false))
			splits.addAll(tableInputFormat.getSplits(context));
		if(conf.getBoolean("file",false)){
			splits.addAll(fileInputFormat.getSplits(context));
		}
		context.getConfiguration().setInt("mapred.reduce.tasks", splits.size());
		return splits;
	}

	public static void newJob() {
		tableInputFormat = new MultiTableInputFormat();
		fileInputFormat = new SequenceFileAsBinaryInputFormat();
	}

	@Override
	public Configuration getConf() {
		if(conf.getBoolean("table",false)){
			return tableInputFormat.getConf();
		}
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		if(conf.getBoolean("table",false)){
			tableInputFormat.setConf(conf);
		}
		this.conf=conf;
	}

	public static void addInputPath(Job job, Path path) throws IOException {
		job.getConfiguration().setBoolean("file", true);
		SequenceFileAsBinaryInputFormat.addInputPaths(job, path.toString());
	}


}
