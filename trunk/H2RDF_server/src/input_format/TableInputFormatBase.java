/**
 * Copyright 2009 The Apache Software Foundation
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import byte_import.HexastoreBulkImport;
import byte_import.MyNewTotalOrderPartitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.io.Text;


/**
 * A base for {@link FileTableInputFormat}s. Receives a {@link HTable}, an 
 * {@link Scan} instance that defines the input columns etc. Subclasses may use 
 * other TableRecordReader implementations.
 * <p>
 * An example of a subclass:
 * <pre>
 *   class ExampleTIF extends TableInputFormatBase implements JobConfigurable {
 *
 *     public void configure(JobConf job) {
 *       HTable exampleTable = new HTable(new HBaseConfiguration(job),
 *         Bytes.toBytes("exampleTable"));
 *       // mandatory
 *       setHTable(exampleTable);
 *       Text[] inputColumns = new byte [][] { Bytes.toBytes("columnA"),
 *         Bytes.toBytes("columnB") };
 *       // mandatory
 *       setInputColumns(inputColumns);
 *       RowFilterInterface exampleFilter = new RegExpRowFilter("keyPrefix.*");
 *       // optional
 *       setRowFilter(exampleFilter);
 *     }
 *
 *     public void validateInput(JobConf job) throws IOException {
 *     }
 *  }
 * </pre>
 */
public abstract class TableInputFormatBase
extends InputFormat<ImmutableBytesWritable, Text> {
  
  final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

  /** The reader scanning the table, can be a custom one. */
  private List<Scan> scanList = null;
  private List<String> tableList = null;
  private List<String> varList = null;
  private List<String> fnameList = null;
  private List<InputSplit> splits = null;
  private HTable table = null;
private static byte[] SUBCLASS = Bytes.toBytes( new Long("8742859611446415633"));
  /**
   * Iterate over an HBase table data, return (ImmutableBytesWritable, Result) 
   * pairs.
   */

  /**
   * Calculates the splits that will serve as input for the map tasks. The
   * number of splits matches the number of regions in a table.
   *
   * @param context  The current job context.
   * @return The list of input splits.
   * @throws IOException When creating the list of splits fails.
   * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(
   *   org.apache.hadoop.mapreduce.JobContext)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
	  
	  Iterator<Scan> scanIterator = scanList.iterator();
	  Iterator<String> tableIterator = tableList.iterator();
	  Iterator<String> varsIterator = varList.iterator();
	  Iterator<String> fnameIterator = fnameList.iterator();
	  
	  System.out.println("calculating splitnumber");

	  Configuration HBconf= HBaseConfiguration.create();
	  Scan scan=null;
	  splits = new ArrayList<InputSplit>();
	  while(scanIterator.hasNext()){ 
	      //System.out.println("New Input BGP");
		  scan=scanIterator.next();
		  String tname = tableIterator.next();
		  table = new HTable( HBconf, tname );
		  String vars=varsIterator.next();
		  String fname=fnameIterator.next();
		  
		  splitSubclass(scan, tname, vars, fname);
	  }
	  if(splits.size()<=HexastoreBulkImport.MAX_TASKS)
		  context.getConfiguration().setInt("mapred.reduce.tasks", splits.size());
	  else
		  context.getConfiguration().setInt("mapred.reduce.tasks", HexastoreBulkImport.MAX_TASKS);
      return splits;
  }
  
	private void splitSubclass(Scan scan, String tname, String vars, String fname) {
		byte[] rowid =scan.getStartRow();
		byte[] startRow = new byte[1+8+8+2];
		byte[] stopRow = new byte[1+8+8+2];
		if (rowid.length==17) {
			byte[] objid = new byte[8];
			for (int i = 0; i < 8; i++) {
				objid[i]=rowid[i+9];
			}
			byte[] classrowStart = new byte[1+8+8+2];
			byte[] classrowStop = new byte[1+8+8+2];
			classrowStart[0]=(byte)3; //pos
			for (int i1 = 0; i1 < 8; i1++) {
				classrowStart[i1+1]=SUBCLASS[i1];
			}
			for (int i1 = 0; i1 < 8; i1++) {
				classrowStart[i1+9]=objid[i1];
			}
			for (int i1 = 0;  i1< classrowStart.length-1; i1++) {
				classrowStop[i1]=classrowStart[i1];
			}
			
			classrowStart[classrowStart.length-2] = (byte) 0;
			classrowStart[classrowStart.length-1] = (byte) 0;
			classrowStop[classrowStop.length-2] = (byte) 255;
			classrowStop[classrowStop.length-1] = (byte) 255;
			
			
			byte[] bid,a;
			a=Bytes.toBytes("A");
			bid = new byte[a.length];
			for (int i = 0; i < a.length; i++) {
				bid[i]=a[i];
			}
			Scan scan1 =new Scan();
			scan1.setStartRow(classrowStart);
			scan1.setStopRow(classrowStop);
			scan1.setCaching(254);
			scan1.addFamily(bid);
			try {
				ResultScanner resultScanner = table.getScanner(scan1);
				Result result = null;
				while((result=resultScanner.next())!=null){
					//System.out.println("Subclasses: "+result.size());
					Iterator<KeyValue> it = result.list().iterator();
					while(it.hasNext()){
						KeyValue kv = it.next();
						byte[] qq = kv.getQualifier();
						for (int ik = 0; ik < 9; ik++) {
							startRow[ik] =rowid[ik];
							stopRow[ik] =rowid[ik];
						}
						for (int ik = 0; ik < 8; ik++) {
							startRow[ik+9] =qq[ik];
							stopRow[ik+9] =qq[ik];
						}
						/*for (int i = 0; i < startRow.length-1; i++) {
							if(i>=9 && i<startRow.length-1){
								startRow[i]=qq[i-9];
								stopRow[i]=qq[i-9];
							}
							else{
								startRow[i]=rowid[i];
								stopRow[i]=rowid[i];
							}
						}*/
						startRow[17] =(byte)0;
						startRow[18] =(byte)0;
						stopRow[17] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
						stopRow[18] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
						//System.out.println(Bytes.toStringBinary(startRow));
						addSplit(tname, vars, fname, startRow, stopRow, scan.getInputColumns());
					}
					
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			
			
			
			/*Get get = new Get(classrow);
			try {
				Result result  = table.get(get);
				System.out.println("Subclasses: "+result.size());
				if(result.size()!=0){
					KeyValue[] vv = result.raw();
					for (int j = 0; j < vv.length; j++) {
						byte[] qq = vv[j].getQualifier();
						
						for (int i = 0; i < startRow.length-1; i++) {
							if(i>=9 && i<startRow.length-1){
								startRow[i]=qq[i-9];
								stopRow[i]=qq[i-9];
							}
							else{
								startRow[i]=rowid[i];
								stopRow[i]=rowid[i];
							}
						}  
						startRow[17] =(byte)0;
						stopRow[17] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
						addSplit(tname, vars, fname, startRow, stopRow, scan.getInputColumns());
					}
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}*/
			
			for (int ik = 0; ik < rowid.length; ik++) {
				startRow[ik] =rowid[ik];
				stopRow[ik] =rowid[ik];
			}
			startRow[17] =(byte)0;
			startRow[18] =(byte)0;
			stopRow[17] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			stopRow[18] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			addSplit(tname, vars, fname, startRow, stopRow, scan.getInputColumns());
						
						
		}
		if (rowid.length==9) {
			for (int ik = 0; ik < rowid.length; ik++) {
				startRow[ik] =rowid[ik];
				stopRow[ik] =rowid[ik];
			}
			for (int ik = 9; ik < startRow.length-2; ik++) {
				startRow[ik] =(byte)0;
				stopRow[ik] =(byte)255;
			}
			startRow[17] =(byte)0;
			startRow[18] =(byte)0;
			stopRow[17] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			stopRow[18] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			addSplit(tname, vars, fname, startRow, stopRow, scan.getInputColumns());
		}
		else if (rowid.length==1){//reverse index scan
			System.out.println("Reverse index scan");
			addSplit(tname, vars, fname, scan.getStartRow(), scan.getStopRow(), scan.getInputColumns());
		}
	}
	

	private void addSplit(String tname, String vars, String fname, byte[] startRow, byte[] stopRow, String incol) {
		
		Pair<byte[][], byte[][]> keys;
		try {
			keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null ||
			        keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			if (table == null) {
				throw new IOException("No table was provided.");
			}
			int count = 0;
			  
			for (int i = 0; i < keys.getFirst().length; i++) {
				String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
				  getServerAddress().getHostname();
				// determine if the given start and stop key fall into the region

				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
				     Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
				    (stopRow.length == 0 ||
				     Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {

				  byte[] splitStart = startRow.length == 0 ||
				    Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
				      keys.getFirst()[i] : startRow;
				  byte[] splitStop = (stopRow.length == 0 ||
				      Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
				      keys.getSecond()[i].length > 0 ?
				        keys.getSecond()[i] : stopRow;
				  //System.out.println("split "+regionLocation+" "+table.getRegionLocation(keys.getFirst()[i]).getRegionInfo().getRegionId());   
				  InputSplit split = new TableColumnSplit(tname, vars, fname,
						  splitStart.clone(), splitStop.clone(), incol, regionLocation);
				  splits.add(split);
				  if (LOG.isDebugEnabled())
				      LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
				  }
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<Scan> getScanList() {
		return scanList;
	}
	
	public void newScanList() {
		scanList = new ArrayList<Scan>();
	}
	
	public void addScan(Scan scan) {
		scanList.add(scan);
	} 
	
	public void setScanList(List<Scan> scanList) {
		this.scanList = scanList;
	}

	public List<String> getTableList() {
		return tableList;
	}

	public void newTableList() {
		tableList = new ArrayList<String>();
		varList = new ArrayList<String>();
		fnameList = new ArrayList<String>();
	}
	
	public void addTable(String table) {
		tableList.add(table);
	} 

	public void addFname(String fname) {
		fnameList.add(fname);
		
	}

	public void addVars(String vars) {
		varList.add(vars);
		
	}
	
	public void setTableList(List<String> tableList) {
		this.tableList = tableList;
	}
	

	public List<String> getFnameList() {
		return fnameList;
	}

	public void setFnameList(List<String> fnameList) {
		this.fnameList = fnameList;
	}

	public List<String> getVarList() {
		return varList;
	}

	public void setVarList(List<String> varList) {
		this.varList = varList;
	}
}
