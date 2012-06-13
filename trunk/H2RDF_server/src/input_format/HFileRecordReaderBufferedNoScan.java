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

import javaewah.EWAHCompressedBitmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;

import com.ibm.icu.util.StringTokenizer;

import partialJoin.BufferedRecordWriter;

import byte_import.MyNewTotalOrderPartitioner;
import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;


public class HFileRecordReaderBufferedNoScan
extends RecordReader<ImmutableBytesWritable, Text> {
	
	  private ImmutableBytesWritable key = null;
	  private Text value = null;
	  private TableColumnSplit tsplit = null;
	  private Reader reader = null;
	  private HFileScanner scanner = null;
	  private KeyValue kv=null;
	  private boolean more=false;
	  private final Configuration HBconf = HBaseConfiguration.create();
	  private byte[] stopr;
	  
	  private static long id;
	  private static int size=0;
	  private static int varsno;
	  private static float progress=0;
	  private static String v1,v2;
	  /**
	   * Closes the split.
	   * 
	   * @see org.apache.hadoop.mapreduce.RecordReader#close()
	   */
	  @Override
	  public void close() {
		  try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
	  
	  HTable table = new HTable( HBconf, tsplit.getTable() );
	  //HTable table = new HTable( HBconf, "H2RDF" );
	  
	  //table.flushCommits();
	  byte[] startr =tsplit.getStartRow();

	  stopr = tsplit.getStopRow();
		System.out.println("start: "+Bytes.toStringBinary(startr));
		System.out.println("stop: "+Bytes.toStringBinary(stopr));
	
	  
	  FileSystem fs = FileSystem.get(HBconf);
	  String dir="";
	  if(startr.length==1){
		  byte[] st = new byte[stopr.length-1];
		  for (int i = 0; i < st.length; i++) {
			  st[i]=stopr[i];
		  }
		  dir ="/hbase/"+tsplit.getTable()+"/"+table.getRegionLocation(stopr).getRegionInfo().getEncodedName()+"/A";
	  }
	  else{
		  dir ="/hbase/"+tsplit.getTable()+"/"+table.getRegionLocation(startr).getRegionInfo().getEncodedName()+"/A";
	  }
	  
      Path regionDir= new Path(dir);
      Path file=null;
      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(regionDir));
      for (Path hfile : hfiles) {
    	  file=new Path(dir+"/"+hfile.getName());
      }
      System.out.println(dir+"/"+file.getName());
      
      reader = HFile.createReader(fs, file, new CacheConfig(HBconf));
      //reader = new Reader(fs, file, null, false);

	  // Load up the index.
	  reader.loadFileInfo();
	  // Get a scanner that caches and that does not use pread.
	  scanner = reader.getScanner(false, true);
	  
	  KeyValue rowKey = KeyValue.createFirstOnRow(startr);
	  scanner.seekBefore(rowKey.getKey());
	  id=table.getRegionLocation(startr).getRegionInfo().getRegionId();
	  if(!scanner.isSeeked()){
		  //System.out.println(table.getRegionLocation(startr).getRegionInfo().getRegionId());
		  scanner.seekTo();
	  }
	  while(scanner.next()){
		  kv = scanner.getKeyValue();
		  if(Bytes.compareTo(kv.getRow(), startr)>=0){
			  more=true;
			  break;
		  }
	  }

	  if(!tsplit.getFname().startsWith("T")){
		  	/*FSDataInputStream v = fs.open(new Path(context.getConfiguration().get("nikos.inputfile")));
		  	BufferedReader read = new BufferedReader(new InputStreamReader(v));
		  	read.readLine();
	  		read.readLine();
	  		v.close();*/
		  
	  		String vars=tsplit.getVars();
	  		System.out.println(vars);
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
	  		progress=(float) 0.2;
	  }
	  else{
		  varsno=15;
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
	  if(!more)
		  return more;
	  if (key == null) key = new ImmutableBytesWritable();
	  if (value == null) value = new Text();
	  if(!scanner.isSeeked()){//bug
		  return false;
	  }

	  try {
		  if(varsno==15){
			  if(Bytes.compareTo(kv.getRow(), stopr)<=0){
				  byte[] indexKey = kv.getRow();
				  byte[] indexKey1 = new byte[ByteValues.totalBytes];
				  for (int i = 0; i < indexKey1.length; i++) {
					  indexKey1[i]=indexKey[i+1];
				  }
				  value.set(tsplit.getFname()+"!"+ByteValues.getStringValue(indexKey1)+"$$"
							  +Bytes.toString(kv.getValue()));
			      more=scanner.next();
			      if(more)
					  kv = scanner.getKeyValue();
				  return more;
			  }
			  return false;
			  
		  }
		  
		  if(varsno==0){ 
			  if(Bytes.compareTo(kv.getRow(), stopr)<=0){
				  key.set(kv.getRow());
				  value.set(tsplit.getFname()+"!"
							  +ByteValues.getStringValue(kv.getValue()));
			      more=scanner.next();
			      if(more)
					  kv = scanner.getKeyValue();
				  return more;
			  }
			  return false;
			  
		  }
		  /*if(kv.getRow()[17]==(byte)255){
			  more=scanner.next();
			  if(more){
				  kv = scanner.getKeyValue();
				  //return nextKeyValue();
			  }
			  return more;
		  }*/
		  if(varsno==1){
			  if(Bytes.compareTo(kv.getRow(), stopr)<=0){
				  value.set(tsplit.getFname()+"!"+v1+"#"
						  +ByteValues.getStringValue(kv.getQualifier())+"_");
			      more=scanner.next();
			      if(more)
					  kv = scanner.getKeyValue();
				  return more;
			  }
			  return false;
		  }
		  else if(varsno==2){
			  if(Bytes.compareTo(kv.getRow(), stopr)<=0){
				  byte[] curkey=kv.getRow();
				  byte[] r1= new byte[ByteValues.totalBytes];
				  for (int j = 0; j < r1.length; j++) {
					  r1[j]=curkey[ByteValues.totalBytes+1+j];
				  }
				  String pat=tsplit.getFname()+"!"+v1+"#"+ByteValues.getStringValue(r1);
				  String buffer="!"+v2+"#"+ByteValues.getStringValue(kv.getQualifier())+"_";
				  boolean flush=false;
				  while(scanner.next() && rowEquals((kv = scanner.getKeyValue()).getRow(), curkey)){
					  buffer+=ByteValues.getStringValue(kv.getQualifier())+"_";
					  if(buffer.length()>=10000){
						  value.set(pat+buffer);
						  flush=true;
						  break;
					  }
				  }
				  if(!flush){
					  value.set(pat+buffer);
				  //System.out.println(pat+buffer);
					  if(!Bytes.equals(kv.getRow(), curkey)){
						  return true;
					  }
					  else{
						  return false;
					  }
				  }
				  else{
					  more=scanner.next();
				      if(more)
						  kv = scanner.getKeyValue();
					  return more;
				  }
			  }
			  return false;
		  }	
	  } catch (NotSupportedDatatypeException e) {
		  throw new InterruptedException("Not supported datatype");
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
    return progress;
  }


}
