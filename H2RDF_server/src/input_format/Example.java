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
package input_format;


import input_format.TableMapReduceUtil;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Example {

	public static class Map extends
			TableMapper<LongWritable, Text> {

		public void map(LongWritable key, Text value,
				Context context) throws IOException, InterruptedException {
			
				context.write(key, value);

		}
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJobName("nikos");

		// disable speculative execution
		job.setJarByClass(Example.class);

		// Set the table name to separate index rows based on where content is
		// stored
		job.getConfiguration().set("TextIndexer.library", "spo");
		// Set the number of reducers for the job
		//job.setNumReduceTasks(numReducers);
		// important! xoris ayto to setting, kollane oi reducers!!!!!
		//job.getConfiguration().setInt("io.sort.mb", 20);
		// space delimined string of column families to scan

		job.setReducerClass(SimpleReducer.class);
		// job.setSortComparatorClass(KeyValue.KeyComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
	    job.setMapperClass(Map.class);
	    job.setInputFormatClass(FileTableInputFormat.class);
	    //job.setInputFormatClass(HFileInputFormat.class);
	    
		FileOutputFormat.setOutputPath(job, new Path("output3"));

		Scan scan =new Scan();
		scan.setStartRow(Bytes.toBytes("873847660^^"));
		scan.setStopRow(Bytes.toBytes("873847660^^999999999"));
		scan.addFamily(Bytes.toBytes("A"));
		HBaseConfiguration HBconf = new HBaseConfiguration();
		HTable table = new HTable( HBconf, "osp" );
		ResultScanner resultScanner = table.getScanner(scan);
		Result result;
		while((result = resultScanner.next())!=null){
			System.out.println(result.toString());
			//System.out.println("hjkhjokhftyfyfgufghjghkgghfghfjgfhj");
		}
		//System.out.println("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii");
		// System.out.println("scan is: " +
		// TableMapReduceUtil.convertScanToString(scan));
		//MyTableMapReduceUtil.addCol("?x", "P0", "spo", "-1496159132", "A", "huihui", job);
		TableMapReduceUtil.newJob();
		//MyTableMapReduceUtil.addRow("?w ?z", "P2", "osp", "982", "982", "A", job);
		//TableMapReduceUtil.addCol("?x", "P0", "spo", "561203963^^", "561203963^^999999999", "A:2086497232", job);
		//TableMapReduceUtil.addRow("?x ?y", "P1", "spo", "947805029^^", "947805029^^999999999", "A", job);
		//TableMapReduceUtil.addRow("?w ?z", "P2", "osp", "893972985^^", "893972985^^999999999", "A", job);
		//TableMapReduceUtil.addRow("?w ?z", "P24", "osp", "9947^^", "9947^^999999999", "A", job);
		MyFileInputFormat.addInputPath(job, new Path("output/BGP1"));
		//MyFileInputFormat.addInputPath(job, new Path("output/BGP0"));
		job.waitForCompletion(true);

	}
}
