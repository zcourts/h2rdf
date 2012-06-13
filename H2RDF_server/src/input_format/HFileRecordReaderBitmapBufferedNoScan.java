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

import java.io.IOException;
import java.util.Iterator;

import javaewah.EWAHCompressedBitmap;

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



public class HFileRecordReaderBitmapBufferedNoScan
extends RecordReader<ImmutableBytesWritable, Text> {
	
	  private ImmutableBytesWritable key = null;
	  private Text value = null;
	  private TableColumnSplit tsplit = null;
	  private Reader reader = null;
	  private HFileScanner scanner = null;
	  private KeyValue kv=null;
	  private boolean more=false;
	  private int regions=0, first;
	  private HBaseConfiguration HBconf = new HBaseConfiguration();
	  private byte[] stopr, startr;
	  private HTable table;
	  private Iterator<Integer> BitmapIter;
	  private static float progress=0;
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
	  
	  
	  EWAHCompressedBitmap regionBitmap = tsplit.getRegionBitmap();
	  BitmapIter = regionBitmap.iterator();
	  //System.out.println("contains: "+regionBitmap.cardinality()+" size:"+regionBitmap.sizeInBytes());
		  
		  
	  table = new HTable( HBconf, tsplit.getTable() );
	  //table.flushCommits();
	  
	  
	  startr =tsplit.getStartRow();

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
	  
	  if(!BitmapIter.hasNext()){
		  more=false;
		  return;
	  }
	  nextBitmapRegion();
	  
	  
	  /*if(!scanner.isSeeked()){
		  //System.out.println(table.getRegionLocation(startr).getRegionInfo().getRegionId());
		  scanner.seekTo();
	  }*/
	  first=1;
	  while(scanner.next()){
		  kv = scanner.getKeyValue();
		  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
			  
			  System.out.println("curkey: "+Bytes.toStringBinary(kv.getRow()));
			  more=true;
			  break;
		  }
		  if(Bytes.compareTo(kv.getRow(), stopr)>0){
			  if(BitmapIter.hasNext()){
				  nextBitmapRegion();
			  }
			  else{
				  more=false;
				  break;
			  }
		  }
	  }
  }

  private void nextBitmapRegion() {
	  regions++;
	  System.out.println("next region: "+regions);
	  int curBitmapInt=BitmapIter.next();
	  byte[] curBitmapByteInt =Bytes.toBytes(curBitmapInt);
	  System.out.println("curInt: "+Bytes.toStringBinary(curBitmapByteInt));
	  startr = new byte[8+1];
	  stopr = new byte[8+1];
	  startr[0]=(byte)1;
	  stopr[0]=(byte)1;
	  for (int j = 1; j < curBitmapByteInt.length+1; j++) {
		  startr[j]=curBitmapByteInt[j-1];
		  stopr[j]=curBitmapByteInt[j-1];
	  }
	  for (int j = 1+curBitmapByteInt.length; j < startr.length; j++) {
		  startr[j]=(byte)0;
		  stopr[j]=(byte)255;
	  }

	  System.out.println("start: "+Bytes.toStringBinary(startr));
	  System.out.println("stop: "+Bytes.toStringBinary(stopr));
	  KeyValue rowKey = KeyValue.createFirstOnRow(startr);
	  try {
		  scanner.seekBefore(rowKey.getKey());
	  } catch (IOException e) {
		e.printStackTrace();
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
	  
	 /* if(!scanner.isSeeked()){//bug
		  return false;
	  }*/
	  if(first==1){
		  if(more){
			  byte[] indexKey = kv.getRow();
			  byte[] indexKey1 = new byte[8];
			  for (int i = 0; i < indexKey1.length; i++) {
				  indexKey1[i]=indexKey[i+1];
			  }
			  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
						  +Bytes.toString(kv.getValue()));
		  }
		  first=2;
		  return more;
	  }

	  while(scanner.next()){
		  kv = scanner.getKeyValue();
		  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
			  byte[] indexKey = kv.getRow();
			  byte[] indexKey1 = new byte[8];
			  for (int i = 0; i < indexKey1.length; i++) {
				  indexKey1[i]=indexKey[i+1];
			  }
			  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
						  +Bytes.toString(kv.getValue()));
			  return true;
		  }
		  if(Bytes.compareTo(kv.getRow(), stopr)>0){
			  if(BitmapIter.hasNext()){
				  nextBitmapRegion();
			  }
			  else{
				  return false;
			  }
		  }
	  }
	  
	  return false;
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
