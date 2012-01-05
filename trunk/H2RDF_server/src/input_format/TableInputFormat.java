/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package input_format;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Convert HBase tabular data into a format that is consumable by Map/Reduce.
 */
public class TableInputFormat extends TableInputFormatBase
implements Configurable {
  
  
  /** Job parameter that specifies the output table. */
  public static final String INPUT_TABLE = "hbase.mapreduce.inputtable";
  /** Space delimited list of columns. */
  public static final String SCAN = "hbase.mapreduce.scan";
  public static final String COLFAMILIES = "hbase.mapreduce.columnfamilies";
  public static final String SCANLIST = "hbase.mapreduce.scanlist";
  public static final String VARS = "hbase.mapreduce.vars";
  public static final String FNAME = "hbase.mapreduce.fname";
  //space delimined list of column families.
  
  /** The configuration. */
  private Configuration conf = null;

  /**
   * Returns the current configuration.
   *  
   * @return The current configuration.
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Sets the configuration. This is used to set the details for the table to
   * be scanned.
   * 
   * @param configuration  The configuration to set.
   * @see org.apache.hadoop.conf.Configurable#setConf(
   *   org.apache.hadoop.conf.Configuration)
   */
  
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    int no=0;
    newScanList();
    newTableList();
    String sc=null;
    while((sc=conf.get(SCANLIST+no))!=null){
    	try {
    		Scan scan = TableMapReduceUtil.convertStringToScan(sc);
			addScan(scan);
			addTable(conf.get(INPUT_TABLE+no));
			addVars(conf.get(VARS+no));
			addFname(conf.get(FNAME+no));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
    	no++;
    }
  }
  
	  public RecordReader<ImmutableBytesWritable, Text> createRecordReader(
	      InputSplit split, TaskAttemptContext context)
	      throws IOException {
	      	return new HFileRecordReaderBufferedScan();
	  }
	  
}
