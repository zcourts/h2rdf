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



public class HFileRecordReaderBitmapCrossRegion
extends RecordReader<ImmutableBytesWritable, Text> {
	
	  private ImmutableBytesWritable key = null;
	  private Text value = null;
	  private TableColumnSplit tsplit = null;
	  private Reader reader = null;
	  private HFileScanner scanner = null;
	  private KeyValue kv=null;
	  private boolean more=false;
	  private int regions=0, first, processed, totalRecords;
	  private byte[] lastRowKey=null;
	  private HBaseConfiguration HBconf = new HBaseConfiguration();
	  private byte[] stopr, startr;
	  private HTable table;
	  private Iterator<Integer> BitmapIter;
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
	  totalRecords=regionBitmap.cardinality();
	  System.out.println("contains: "+regionBitmap.cardinality()+" size:"+regionBitmap.sizeInBytes());
		  
		  
	  table = new HTable( HBconf, tsplit.getTable() );
	  //table.flushCommits();
	  processed=0;
	  nextBitmapRegion();
  }

  private void nextBitmapRegion() {
	  regions++;
	  //System.out.println("next region: "+regions);
	  if(!BitmapIter.hasNext()){
		  System.out.println("Bitmap end");
		  more=false;
		  return;
	  }
	  int curBitmapInt=BitmapIter.next();
	  processed++;
	  //System.out.println(curBitmapInt);
		  
	  byte[] curBitmapByteInt =Bytes.toBytes(curBitmapInt);
	  //System.out.println("curInt: "+Bytes.toStringBinary(curBitmapByteInt));
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

	  //System.out.println("Next Bitmap start: "+Bytes.toStringBinary(startr));
	  
	  KeyValue rowKey = KeyValue.createFirstOnRow(startr);
	  try {
		  table.flushCommits();
		  if(lastRowKey==null){//read the first HFile of the bitmap
			  FileSystem fs = FileSystem.get(HBconf);
			  String dir="";
			  dir ="/hbase/"+tsplit.getTable()+"/"+table.getRegionLocation(startr).getRegionInfo().getEncodedName()+"/A";
		      Path regionDir= new Path(dir);
		      Path file=null;
		      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(regionDir));
		      for (Path hfile : hfiles) {
		    	  file=new Path(dir+"/"+hfile.getName());
		      }
		      //System.out.println(dir);
		      System.out.println("First HFile: "+dir+"/"+file.getName());
		      
		      reader = HFile.createReader(fs, file, new CacheConfig(HBconf));
		      //reader = new Reader(fs, file, null, false);
	
			  // Load up the index.
		      lastRowKey = table.getRegionLocation(startr).getRegionInfo().getEndKey();
		      reader.loadFileInfo();
			  // Get a scanner that caches and that does not use pread.
			  scanner = reader.getScanner(false, true);
			  scanner.seekBefore(rowKey.getKey());
			  first=1;
			  while(scanner.next()){
				  kv = scanner.getKeyValue();
				  //System.out.println("first key in Hfile: "+Bytes.toStringBinary(kv.getRow()));
				  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
					  
					  //System.out.println("curkey: "+Bytes.toStringBinary(kv.getRow()));
					  more=true;
					  break;
				  }
				  if(Bytes.compareTo(kv.getRow(), stopr)>0){
					  if(BitmapIter.hasNext()){
						  nextBitmapRegion();
					  }
					  else{
						  System.out.println("Bitmap end");
						  more=false;
						  break;
					  }
				  }
			  }
		  }
		  else{
			  if(Bytes.compareTo(lastRowKey, startr)>0){//same region 
				  scanner.seekBefore(rowKey.getKey());
				  //System.out.println("Same region");
				  
				  first=1;
				  while(scanner.next()){
					  kv = scanner.getKeyValue();
					  //System.out.println("next key: "+Bytes.toStringBinary(kv.getRow()));
					  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
						  
						  //System.out.println("curkey: "+Bytes.toStringBinary(kv.getRow()));
						  more=true;
						  break;
					  }
					  if(Bytes.compareTo(kv.getRow(), stopr)>0){
						  if(BitmapIter.hasNext()){
							  nextBitmapRegion();
						  }
						  else{
							  System.out.println("Bitmap end");
							  more=false;
							  break;
						  }
					  }
				  }
			  }
			  else{//open new HFile 
				  FileSystem fs = FileSystem.get(HBconf);
				  String dir="";
				  dir ="/hbase/"+tsplit.getTable()+"/"+table.getRegionLocation(startr).getRegionInfo().getEncodedName()+"/A";
			      Path regionDir= new Path(dir);
			      Path file=null;
			      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(regionDir));
			      for (Path hfile : hfiles) {
			    	  file=new Path(dir+"/"+hfile.getName());
			      }
			      System.out.println("New Hfile"+dir+"/"+file.getName());
			      
			      reader = HFile.createReader(fs, file, new CacheConfig(HBconf));
			      //reader = new Reader(fs, file, null, false);
		
				  // Load up the index.
			      lastRowKey = table.getRegionLocation(startr).getRegionInfo().getEndKey();
			      
				  reader.loadFileInfo();
				  // Get a scanner that caches and that does not use pread.
				  scanner = reader.getScanner(false, true);
				  scanner.seekBefore(rowKey.getKey());
				  first=1;
				  while(scanner.next()){
					  kv = scanner.getKeyValue();
					  //System.out.println("first key in Hfile: "+Bytes.toStringBinary(kv.getRow()));
					  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
						  
						  //System.out.println("curkey: "+Bytes.toStringBinary(kv.getRow()));
						  more=true;
						  break;
					  }
					  if(Bytes.compareTo(kv.getRow(), stopr)>0){
						  if(BitmapIter.hasNext()){
							  nextBitmapRegion();
						  }
						  else{
							  System.out.println("Bitmap end");
							  more=false;
							  break;
						  }
					  }
				  }
			  }
		  }
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
	  
	  if(first==1){
		  if(more){
			  byte[] indexKey = kv.getRow();
			  byte[] indexKey1 = new byte[8];
			  for (int i = 0; i < indexKey1.length; i++) {
				  indexKey1[i]=indexKey[i+1];
			  }
			  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
						  +Bytes.toString(kv.getValue()));
			  //System.out.println("used key: "+Bytes.toStringBinary(kv.getRow()));
		  }
		  first=2;
		  return more;
	  }

	  if(!more)
		  return more;

	  if(!scanner.isSeeked()){//bug
		  System.out.println("Not seeked");
		  return nextHfile();
	  }
	  
	  if(scanner.next()){
		  kv = scanner.getKeyValue();
	      //System.out.println("next key: "+Bytes.toStringBinary(kv.getRow()));
	      
		  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
			  byte[] indexKey = kv.getRow();
			  byte[] indexKey1 = new byte[8];
			  for (int i = 0; i < indexKey1.length; i++) {
				  indexKey1[i]=indexKey[i+1];
			  }
			  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
						  +Bytes.toString(kv.getValue()));
			  //System.out.println("used key: "+Bytes.toStringBinary(kv.getRow()));
			  return true;
		  }
		  if(Bytes.compareTo(kv.getRow(), stopr)>0){
			  if(BitmapIter.hasNext()){
				  nextBitmapRegion();
				  
				  if(!scanner.isSeeked()){//bug
					  System.out.println("Not seeked");
					  return nextHfile();
				  }
				  if(more){
					  byte[] indexKey = kv.getRow();
					  byte[] indexKey1 = new byte[8];
					  for (int i = 0; i < indexKey1.length; i++) {
						  indexKey1[i]=indexKey[i+1];
					  }
					  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
								  +Bytes.toString(kv.getValue()));
					  //System.out.println("used key: "+Bytes.toStringBinary(kv.getRow()));
				  }
				  first=2;
				  return more;
			  }
			  else{
				  System.out.println("Bitmap end");
				  return false;
			  }
		  }
		  else{
			  System.out.println("Bug");
			  return false;
		  }
	  }
	  
	  //next region
	  return nextHfile();
  }
  
private boolean nextHfile() {
	FileSystem fs;
	try {
		fs = FileSystem.get(HBconf);
		String dir="";
	  	dir ="/hbase/"+tsplit.getTable()+"/"+table.getRegionLocation(lastRowKey).getRegionInfo().getEncodedName()+"/A";
	    Path regionDir= new Path(dir);
	    Path file=null;
	    Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(regionDir));
	    for (Path hfile : hfiles) {
	  	  file=new Path(dir+"/"+hfile.getName());
	    }
	    //System.out.println("Last row key"+Bytes.toStringBinary(lastRowKey));
	    System.out.println("HFile: "+dir+"/"+file.getName());
	    
	    reader = HFile.createReader(fs, file, new CacheConfig(HBconf));
	    //reader = new Reader(fs, file, null, false);
	
		  // Load up the index.
	    lastRowKey = table.getRegionLocation(lastRowKey).getRegionInfo().getEndKey();
		  reader.loadFileInfo();
		  // Get a scanner that caches and that does not use pread.
		  scanner = reader.getScanner(false, true);
		  scanner.seekTo();
		  first=1;
		  if (scanner.next()){
			  
			  kv = scanner.getKeyValue();
		      //System.out.println("next key after change: "+Bytes.toStringBinary(kv.getRow()));
		      
			  if(Bytes.compareTo(kv.getRow(), startr)>=0 && Bytes.compareTo(kv.getRow(), stopr)<=0){
				  byte[] indexKey = kv.getRow();
				  byte[] indexKey1 = new byte[8];
				  for (int i = 0; i < indexKey1.length; i++) {
					  indexKey1[i]=indexKey[i+1];
				  }
				  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
							  +Bytes.toString(kv.getValue()));
				  //System.out.println("used key: "+Bytes.toStringBinary(kv.getRow()));
				  return true;
			  }
			  if(Bytes.compareTo(kv.getRow(), stopr)>0){
				  if(BitmapIter.hasNext()){
					  nextBitmapRegion();
					  
					  if(!scanner.isSeeked()){//bug
						  System.out.println("Not seeked");
						  return nextHfile();
					  }
					  if(more){
						  byte[] indexKey = kv.getRow();
						  byte[] indexKey1 = new byte[8];
						  for (int i = 0; i < indexKey1.length; i++) {
							  indexKey1[i]=indexKey[i+1];
						  }
						  value.set(tsplit.getFname()+"!"+Bytes.toLong(indexKey1)+"$$"
									  +Bytes.toString(kv.getValue()));
						  //System.out.println("used key after change: "+Bytes.toStringBinary(kv.getRow()));
					  }
					  first=2;
					  return more;
				  }
				  else{
					  System.out.println("Bitmap end");
					  return false;
				  }
			  }
			  else{
				  System.out.println("Bug");
				  return false;
			  }
		  }
		  
	} catch (IOException e) {
		e.printStackTrace();
	}
	System.out.println("Bug");
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
	float progress =  (float) processed/totalRecords;
	if(progress<1)
		return progress;
	else
		return new Float(1.0);
  }


}
