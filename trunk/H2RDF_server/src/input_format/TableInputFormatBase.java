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

import javaewah.EWAHCompressedBitmap;


import byte_import.HexastoreBulkImport;
import byte_import.MyNewTotalOrderPartitioner;
import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
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

import translator.CastLongToInt;


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
  private Configuration conf;
  private EWAHCompressedBitmap ewahBitmap;
  private final int totsize= ByteValues.totalBytes, rowlength=1+2*totsize;
  private int max_tasks=1;
  private Pair<byte[][], byte[][]> keys;
  
  
private static byte[] SUBCLASS;//Bytes.toBytes( new Long("8742859611446415633"));
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
	  
	 // String p=context.getConfiguration().get("mapred.fairscheduler.pool");
	  //max_tasks = Integer.parseInt(p.substring(p.indexOf("l")+1));
	  max_tasks=1000;
	  Iterator<Scan> scanIterator = scanList.iterator();
	  Iterator<String> tableIterator = tableList.iterator();
	  Iterator<String> varsIterator = varList.iterator();
	  Iterator<String> fnameIterator = fnameList.iterator();
	  
	  try {
		  SUBCLASS = ByteValues.getFullValue("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
	  } catch (NotSupportedDatatypeException e) {
		  throw new IOException("Not supported datatype");
	  }
	  System.out.println("calculating splitnumber");
	  conf = context.getConfiguration();
	  Configuration HBconf= HBaseConfiguration.create();
	  Scan scan=null;
	  splits = new ArrayList<InputSplit>();
	  while(scanIterator.hasNext()){ 
	      System.out.println("New Input BGP");
		  scan=scanIterator.next();
		  String tname = tableIterator.next();
		  table = new HTable( HBconf, tname );
		  keys = table.getStartEndKeys();
		  String vars=varsIterator.next();
		  String fname=fnameIterator.next();
		  
		  splitSubclass(scan, tname, vars, fname);
	  }
	  if(splits.size()<=max_tasks)
		  context.getConfiguration().setInt("mapred.reduce.tasks", splits.size());
	  else
		  context.getConfiguration().setInt("mapred.reduce.tasks", max_tasks);
      return splits;
  }
  
	private void splitSubclass(Scan scan, String tname, String vars, String fname) {
		byte[] rowid =scan.getStartRow();
		String col="";
		if(scan.hasFamilies()){
			col=Bytes.toString(scan.getFamilies()[0]);
		}
		//System.out.println(Bytes.toString(scan.getFamilies()[0]));
		byte[] startRow = new byte[rowlength+2];
		byte[] stopRow = new byte[rowlength+2];
		if (scan.getFamilies()[0].length<=1){//rowid.length==rowlength) {
			byte[] objid = new byte[totsize];
			for (int i = 0; i < totsize; i++) {
				objid[i]=rowid[i+totsize+1];
			}
			byte[] classrowStart = new byte[rowlength+2];
			byte[] classrowStop = new byte[rowlength+2];
			classrowStart[0]=(byte)3; //pos
			for (int i1 = 0; i1 < totsize; i1++) {
				classrowStart[i1+1]=SUBCLASS[i1];
			}
			for (int i1 = 0; i1 < totsize; i1++) {
				classrowStart[i1+totsize+1]=objid[i1];
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
					System.out.println("Subclasses: " +result.size());
					Iterator<KeyValue> it = result.list().iterator();
					while(it.hasNext()){
						KeyValue kv = it.next();
						byte[] qq = kv.getQualifier();
						for (int ik = 0; ik < totsize+1; ik++) {
							startRow[ik] =rowid[ik];
							stopRow[ik] =rowid[ik];
						}
						for (int ik = 0; ik < totsize; ik++) {
							startRow[ik+totsize+1] =qq[ik];
							stopRow[ik+totsize+1] =qq[ik];
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
						startRow[rowlength] =(byte)0;
						startRow[rowlength+1] =(byte)0;
						stopRow[rowlength] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
						stopRow[rowlength+1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
						//System.out.println(Bytes.toStringBinary(startRow));
						//addSplit(tname, vars, fname, startRow, stopRow, col);
						scan.setStartRow(startRow);
						scan.setStopRow(stopRow);
						splitSubclass(scan, tname, vars, fname);
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
			startRow[rowlength] =(byte)0;
			startRow[rowlength+1] =(byte)0;
			stopRow[rowlength] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			stopRow[rowlength+1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			addSplit(tname, vars, fname, startRow, stopRow, col);
						
						
		}
		else if (scan.hasFamilies()){
			if(rowid.length==totsize+1) {
				for (int ik = 0; ik < rowid.length; ik++) {
					startRow[ik] =rowid[ik];
					stopRow[ik] =rowid[ik];
				}
				for (int ik = totsize+1; ik < startRow.length-2; ik++) {
					startRow[ik] =(byte)0;
					stopRow[ik] =(byte)255;
				}
				startRow[rowlength] =(byte)0;
				startRow[rowlength+1] =(byte)0;
				stopRow[rowlength] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				stopRow[rowlength+1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				addSplit(tname, vars, fname, startRow, stopRow, col);
			}
			else{
				byte[] stop = scan.getStopRow();
				if(stop.length<=1)
					stop=rowid;
				System.out.println(Bytes.toStringBinary(stop));
				startRow[0] =rowid[0];
				stopRow[0] =stop[0];
				for (int i = 1; i < 1+2*totsize; i++) {
					startRow[i] =rowid[i];
					stopRow[i] =stop[i];
				}
				startRow[rowlength] =(byte)0;
				startRow[rowlength+1] =(byte)0;
				stopRow[rowlength] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				stopRow[rowlength+1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				
				System.out.println(Bytes.toStringBinary(startRow));
				System.out.println(Bytes.toStringBinary(stopRow));
				addSplit(tname, vars, fname, startRow, stopRow, col);
			}
		}
		else if (rowid.length==1){//reverse index scan
			System.out.println("Reverse index scan");
			String bitmapDir = conf.get("nikos.inputfile");
			FileSystem fs;
			try {
				fs = FileSystem.get(conf);
				Path[] bitmapFiles = FileUtil.stat2Paths(fs.listStatus(new Path(bitmapDir)));
				ewahBitmap = new EWAHCompressedBitmap();
			    for (Path bitmapFile : bitmapFiles) {
					//System.out.println(bitmapFile);
			    	
					EWAHCompressedBitmap ewahBitmapTemp = new EWAHCompressedBitmap();
					ewahBitmapTemp.deserialize(fs.open(bitmapFile));
					
				    //System.out.println("bitmap size in bytes: "+ewahBitmapTemp.sizeInBytes());
				    //System.out.println("cardinality: "+ewahBitmapTemp.cardinality());
				    ewahBitmap=ewahBitmap.or(ewahBitmapTemp);
				    //System.out.println("Total bitmap size in bytes: "+ewahBitmap.sizeInBytes());
				    //System.out.println("Total cardinality: "+ewahBitmap.cardinality());
			    }
			    //System.out.println("Total bitmap size in bytes: "+ewahBitmap.sizeInBytes());
			    //System.out.println("Total cardinality: "+ewahBitmap.cardinality());
			   
			    int card = ewahBitmap.cardinality()/max_tasks +1;
			    Iterator<Integer> bitmapIt = ewahBitmap.iterator();
			    int size = 0, numSplits=0, firstOfRegion=0;
			    String[] slaves = {"clone22", "clone29"};
			    EWAHCompressedBitmap ewahBitmapTemp = new EWAHCompressedBitmap();
			    while(bitmapIt.hasNext()){
			    	if(size==0){
			    		firstOfRegion=bitmapIt.next();
			    		ewahBitmapTemp.set(firstOfRegion);
			    	}
			    	else{
			    		ewahBitmapTemp.set(bitmapIt.next());
			    	}
			    	size++;
			    	if(size>=card){
			    		numSplits++;
			    		String regionlocation= slaves[numSplits%2];
			    		byte [] nextValueByteInt =Bytes.toBytes(firstOfRegion);
						
						  //byte[] nextValueByte = Bytes.toBytes(new Long(nextValue*Integer.MAX_VALUE)) ;
			    		byte []nextKey = new byte[totsize+1];
			    		nextKey[0]=(byte)1;
						for (int j = 1; j < nextValueByteInt.length; j++) {
							nextKey[j]=nextValueByteInt[j-1];
						}
						for (int j = 1+nextValueByteInt.length; j < nextKey.length; j++) {
							nextKey[j]=(byte)0;
						}
			    		//String regionlocation = fs.getFileBlockLocations(fs.getFileStatus(bitmapFile), new Long(0), fs.getFileStatus(bitmapFile).getLen())[0].getHosts()[0];
						//System.out.println("regionlocation: "+regionlocation);
						//System.out.println("Adding split "+numSplits);
						//System.out.println("Cardinality "+ewahBitmapTemp.cardinality());
						
						InputSplit split = new TableColumnSplit(tname, vars, fname,
								nextKey.clone(), scan.getStopRow().clone(), col, regionlocation , ewahBitmapTemp);

						splits.add(split);
					    size = 0;
					    ewahBitmapTemp = new EWAHCompressedBitmap();
			    	}
			    }
			    if(size!=0){
		    		numSplits++;
		    		String regionlocation= slaves[numSplits%2];
			    	byte [] nextValueByteInt =Bytes.toBytes(firstOfRegion);
					
					  //byte[] nextValueByte = Bytes.toBytes(new Long(nextValue*Integer.MAX_VALUE)) ;
		    		byte []nextKey = new byte[totsize+1];
		    		nextKey[0]=(byte)1;
					for (int j = 1; j < nextValueByteInt.length; j++) {
						nextKey[j]=nextValueByteInt[j-1];
					}
					for (int j = 1+nextValueByteInt.length; j < nextKey.length; j++) {
						nextKey[j]=(byte)0;
					}
		    		//String regionlocation = fs.getFileBlockLocations(fs.getFileStatus(bitmapFile), new Long(0), fs.getFileStatus(bitmapFile).getLen())[0].getHosts()[0];
					//System.out.println("regionlocation: "+regionlocation);
					//System.out.println("Adding split "+numSplits);
					//System.out.println("Cardinality "+ewahBitmapTemp.cardinality());
					
					InputSplit split = new TableColumnSplit(tname, vars, fname,
							nextKey.clone(), scan.getStopRow().clone(), col, regionlocation , ewahBitmapTemp);

					splits.add(split);
			    }
			   
			    
			    
			    
			} catch (IOException e) {
				e.printStackTrace();
			}
			//addSplitIndexTranslate(tname, vars, fname, scan.getStartRow(), scan.getStopRow(), col);
		}
	}
	
	
	private void addSplitIndexTranslate2(String tname, String vars, String fname, byte[] startRow, byte[] stopRow, String incol) {

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
					
					byte[]regStart = keys.getFirst()[i];
					byte[]regStop = keys.getSecond()[i];
					byte[]regStartId = new byte[totsize];
					byte[]regStopId = new byte[totsize];
					
					for (int j = 0; j < totsize; j++) {
						if(j+1>=regStart.length){
							regStartId[j]=(byte)0;
						}
						else{
							regStartId[j]=regStart[j+1];
						}
						if(j+1>=regStop.length){
							regStopId[j]=(byte)0;
						}
						else{
							regStopId[j]=regStop[j+1];
						}
					}
						
					
					Integer start=CastLongToInt.castLong(Bytes.toLong(regStartId));
					if (keys.getSecond()[i][0]==(byte)2){
						start=Integer.MAX_VALUE;
					}
					Integer stop =CastLongToInt.castLong(Bytes.toLong(regStopId));
					EWAHCompressedBitmap regionBitmap = new EWAHCompressedBitmap();
					//System.out.println("used region start: "+Bytes.toLong(regStartId)+" stop: "+Bytes.toLong(regStopId));
					//System.out.println("used region start: "+start+" stop: "+stop);
					
					for (int j = start; j <= stop; j++) {
						regionBitmap.set(j);
					}
					regionBitmap=ewahBitmap.and(regionBitmap);
					int card=regionBitmap.cardinality();
					if(card>0){
						System.out.println("used region start: "+start+" stop: "+stop);
						System.out.println("contains:"+card);
						System.out.println("size in bytes:"+regionBitmap.sizeInBytes());
						count++;
						System.out.println("split "+count+": "+regionLocation+" "+table.getRegionLocation(keys.getFirst()[i]).getRegionInfo().getEncodedName());   
						InputSplit split = new TableColumnSplit(tname, vars, fname,
								regStart.clone(), regStop.clone(), incol, regionLocation, regionBitmap);
						  
						//System.out.println("Adding split "+count);
						splits.add(split);
						if (LOG.isDebugEnabled())
						    LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
					}
					else{
						System.out.println("unused region start: "+start+" stop: "+stop);
					}
						  
				}		  
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	private void addSplitIndexTranslate(String tname, String vars, String fname, byte[] startRow, byte[] stopRow, String incol) {

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
			
			Iterator<Integer> bitmapIterator = ewahBitmap.iterator();
			int nextValue;
			if(!bitmapIterator.hasNext()){
				return;
			}
			nextValue=bitmapIterator.next();
			byte[] nextValueByteInt =Bytes.toBytes(nextValue);
			
			//byte[] nextValueByte = Bytes.toBytes(new Long(nextValue*Integer.MAX_VALUE)) ;
			byte[] nextKey = new byte[totsize+1];
			nextKey[0]=(byte)1;
			for (int j = 1; j < nextValueByteInt.length+1; j++) {
				nextKey[j]=nextValueByteInt[j-1];
			}
			for (int j = 1+nextValueByteInt.length; j < nextKey.length; j++) {
				nextKey[j]=(byte)255;
			}
			
			
			for (int i = 0; i < keys.getFirst().length; i++) {
				String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
				  getServerAddress().getHostname();
				// determine if the given start and stop key fall into the region

				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
				     Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
				    (stopRow.length == 0 ||
				     Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {

					  
					  if(Bytes.compareTo(keys.getSecond()[i], nextKey) <= 0 && keys.getSecond()[i].length > 0){//unused region
						  System.out.println("unused region");
						  continue;
					  }
					  else{//used region find next key that is greater than its end
						  System.out.println("used region");
						  byte[] splitStart= nextKey.clone();//startrow the first key
						  byte[] splitStop = nextKey.clone();
						  //find the first key that falls out of the region
						  int ell=0;

						  EWAHCompressedBitmap regionBitmap = new EWAHCompressedBitmap();
						  regionBitmap.set(nextValue);
						  while(bitmapIterator.hasNext()){
							  ell++;
							  nextValue=bitmapIterator.next();
							  nextValueByteInt =Bytes.toBytes(nextValue);
								
							  //byte[] nextValueByte = Bytes.toBytes(new Long(nextValue*Integer.MAX_VALUE)) ;
							  nextKey = new byte[totsize+1];
							  nextKey[0]=(byte)1;
							  for (int j = 1; j < nextValueByteInt.length; j++) {
								  nextKey[j]=nextValueByteInt[j-1];
							  }
							  for (int j = 1+nextValueByteInt.length; j < nextKey.length; j++) {
								  nextKey[j]=(byte)255;
							  }
							  splitStop=nextKey.clone();

							  regionBitmap.set(nextValue);
							  if(Bytes.compareTo(keys.getSecond()[i], nextKey) <= 0 && keys.getSecond()[i].length > 0){
								  break;
							  }
						  }
						  
						  
						  System.out.println("contains:"+ell);
						  count++;
						  System.out.println("split "+count+": "+regionLocation+" "+table.getRegionLocation(keys.getFirst()[i]).getRegionInfo().getEncodedName());   
						  InputSplit split = new TableColumnSplit(tname, vars, fname,
								  splitStart.clone(), splitStop.clone(), incol, regionLocation, regionBitmap);
						  
						  //System.out.println("Adding split "+count);
						  splits.add(split);
						  if (LOG.isDebugEnabled())
						      LOG.debug("getSplits: split -> " + (count++) + " -> " + split);
						  
						  if(!bitmapIterator.hasNext()){
							  System.out.println("No more ellements in bitmap");
							  return;
						  }
					  }
					  
					  
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	private void addSplit(String tname, String vars, String fname, byte[] startRow, byte[] stopRow, String incol) {
		System.out.println("Adding split");
		try {
			if (keys == null || keys.getFirst() == null ||
			        keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			if (table == null) {
				throw new IOException("No table was provided.");
			}
			int count = 0;
			  
			for (int i = 0; i < keys.getFirst().length; i++) {
				//String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
				//  getServerAddress().getHostname();
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

					String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
							  getServerAddress().getHostname();
					  //System.out.println("split "+regionLocation+" "+table.getRegionLocation(keys.getFirst()[i]).getRegionInfo().getRegionId());   
					  InputSplit split = new TableColumnSplit(tname, vars, fname,
							  splitStart.clone(), splitStop.clone(), incol, regionLocation, new EWAHCompressedBitmap());
					  
					  //System.out.println("Adding split "+count);
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
