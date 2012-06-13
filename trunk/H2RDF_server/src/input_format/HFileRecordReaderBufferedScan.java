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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;

import byte_import.MyNewTotalOrderPartitioner;
import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

public class HFileRecordReaderBufferedScan
extends RecordReader<ImmutableBytesWritable, Text> {
  
  private ResultScanner resultScanner =null;
  private Result result =null;
  private Iterator<KeyValue> list = null;
  private ImmutableBytesWritable key = null;
  private Text value = null;
  private TableColumnSplit tsplit = null;
  private Scan scan = null;
  private KeyValue kv=null;
  private final Configuration HBconf = HBaseConfiguration.create();
  private boolean empty, more;
  private int varsno; 
  private String v1, v2;
  /**
   * Closes the split.
   * 
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() {
	resultScanner.close();
  }

  /**
   * Returns the current key.
   *  
   * @return The current key.
   * @throws IOException
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
   */
  @Override
  public ImmutableBytesWritable getCurrentKey() throws IOException,
      InterruptedException {
    return key;
  }

  /**
   * Returns the current value.
   * 
   * @return The current value.
   * @throws IOException When the value is faulty.
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  /**
   * Initializes the reader.
   * 
   * @param inputsplit  The split to work with.
   * @param context  The current task context.
   * @throws IOException When setting up the reader fails.
   * @throws InterruptedException When the job is aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
   *   org.apache.hadoop.mapreduce.InputSplit, 
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(InputSplit inputsplit,
	  TaskAttemptContext context) throws IOException,
	  InterruptedException {
	  tsplit=(TableColumnSplit) inputsplit;
	  scan = new Scan();
	  /*byte[] rowid =tsplit.getStartRow();
	  byte[] startr = new byte[19];
	  byte[] stopr = new byte[19];
	  for (int i = 0; i < rowid.length; i++) {
		  startr[i] =rowid[i];
		  stopr[i] =rowid[i];
	  }
	  if (rowid.length==18) {
		  startr[18] =(byte)0;
		  stopr[18] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
	  }
	  if (rowid.length==10) {
		  for (int i = 10; i < startr.length-1; i++) {
			  startr[i] =(byte)0;
			  stopr[i] =(byte)255;
		  }
		  startr[startr.length-1] =(byte)0;
		  stopr[startr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
	  }*/
	  
	  scan.setStartRow(tsplit.getStartRow());
	  scan.setStopRow(tsplit.getStopRow());
	  //scan.setCaching(1);//50
	  scan.setBatch(11000);
	  byte[] a, bid=null;
	  a=Bytes.toBytes("A");
	  bid = new byte[a.length];
	  for (int i = 0; i < a.length; i++) {
		  bid[i]=a[i];
	  }
	  
		//System.out.println(Bytes.toStringBinary(bid));
		scan.addFamily(bid);
	  
		HTable table = new HTable( HBconf, tsplit.getTable() );
		resultScanner = table.getScanner(scan);
		
		//System.out.println(Bytes.toStringBinary(scan.getStartRow()));
		//System.out.println(Bytes.toStringBinary(scan.getStopRow()));
		/*
		System.out.println(Bytes.toString(Bytes.toBytes(scan.getInputColumns())));
		Get get = new Get(scan.getStartRow());
		Result re;
		System.out.println("iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii");
		while((re = resultScanner.next())!=null){
			System.out.println("o");
			System.out.println(re.size());
        	//System.out.println(String.format("%s$$%s ", var1, Bytes.toString(list.next().getQualifier())));
		}
		System.exit(1);*/
		
	  result = resultScanner.next();
	  more=false;
	  if(result==null){
		  empty=true;
	  }
	  else{
		  more=true;
		  list = result.list().iterator();
		  kv=list.next();
	  }
	  
	  Configuration conf = context.getConfiguration();
	  String newjoinVars=conf.get("input.patId");
	  String joinVars = newjoinVars.split(tsplit.getFname())[1];
	  joinVars=joinVars.substring(0, joinVars.indexOf("$$")-1);
	  String vars=tsplit.getVars();
	  StringTokenizer vtok = new StringTokenizer(vars);
	  varsno=0;
	  while(vtok.hasMoreTokens()){
		  vtok.nextToken();
		  varsno++;
	  }
	  if(varsno==1){
		  StringTokenizer vtok2 = new StringTokenizer(vars);
		  v1=vtok2.nextToken();
	  }
	  else if(varsno==2){
		  StringTokenizer vtok2 = new StringTokenizer(vars);
		  v1=vtok2.nextToken();
		  v2=vtok2.nextToken();
	  }
  }


  /**
   * Positions the record reader to the next record.
   *  
   * @return <code>true</code> if there was another record.
   * @throws IOException When reading the record failed.
   * @throws InterruptedException When the job was aborted.
   * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
	  if (key == null) key = new ImmutableBytesWritable();
	  if (value == null) value = new Text();
	  if(!more)
		  return refresh();

	  try {
		  if(varsno==1){
			value.set(tsplit.getFname()+"!"+v1+"#"
								  +ByteValues.getStringValue(kv.getQualifier())+"_");
		      more=list.hasNext();
		      if(more)
				  kv = list.next();
			  return refresh();
		  }
		  else if(varsno==2){
				  byte[] curkey=kv.getRow().clone();
				  byte[] r1= new byte[ByteValues.totalBytes];
				  for (int j = 0; j < r1.length; j++) {
					  r1[j]=curkey[ByteValues.totalBytes+1+j];
				  }
				  String pat=tsplit.getFname()+"!"+v1+"#"+ByteValues.getStringValue(r1);
				  String buffer="!"+v2+"#"+ByteValues.getStringValue(kv.getQualifier())+"_";
				  boolean flush=false;
				  while(list.hasNext() && rowEquals((kv = list.next()).getRow(), curkey)){
					  //if(kv.getRow()[17]!=(byte)255){
						  buffer+=ByteValues.getStringValue(kv.getQualifier())+"_";
						  if(buffer.length()>=10000){
							  value.set(pat+buffer);
							  flush=true;
							  break;
						  }
					  //}
				  }
				  if(!flush){
					  value.set(pat+buffer);
					  if(!Bytes.equals(kv.getRow(), curkey)){
						  more=true;
						  return more;
					  }
					  else{
						  more=false;
						  return refresh();
					  }
				  }
				  else{
					  more=list.hasNext();
				      if(more)
						  kv = list.next();
					  return refresh();//&& kv.getRow()[17]!=(byte)255;
				  }
		  }	
	  } catch (NotSupportedDatatypeException e) {
		  throw new InterruptedException("Not supported datatype");
	  }
	  return false; 
  }
	  
	  
  
  private boolean refresh() {
	  if (more)
		  return more;
	  else{
		  try {
		  	result = resultScanner.next();
			if(result!=null){
				more=true;
				list = result.list().iterator();
				kv=list.next();
			}
			return more;
		  } catch (IOException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
		  
	  }
	  return false;
  }

private boolean rowEquals(byte[] row, byte[] curkey) {
	  boolean ret=true;
	  for (int i = 0; i < 1+2*ByteValues.totalBytes; i++) {
		  if(row[i]!=curkey[i]){
			  ret=false;
			  break;
		  }
	  }
	  return ret;
  }
  /**
   * The current progress of the record reader through its data.
   * 
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    // Depends on the total number of tuples
    return 0;
  }


}
