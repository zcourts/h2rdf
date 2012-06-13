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
import java.util.StringTokenizer;

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

import byte_import.MyNewTotalOrderPartitioner;


public class HFileRecordReaderNoScan
extends RecordReader<ImmutableBytesWritable, Text> {

	  private ImmutableBytesWritable key = null;
	  private Text value = null;
	  private TableColumnSplit tsplit = null;
	  private Reader reader = null;
	  private HFileScanner scanner = null;
	  private KeyValue kv=null;
	  private boolean more;
	  private HBaseConfiguration HBconf = new HBaseConfiguration();
	  private byte[] stopr;
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
	  byte[] rowid =tsplit.getStartRow();
	  byte[] startr = new byte[19];
	  stopr = new byte[19];
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
	  }
	  

	  FileSystem fs = FileSystem.get(HBconf);
	  String dir ="/hbase/"+tsplit.getTable()+"/"+table.getRegionLocation(startr).getRegionInfo().getEncodedName()+"/A";
      Path regionDir= new Path(dir);
      Path file=null;
      Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(regionDir));
      for (Path hfile : hfiles) {
    	  file=new Path(dir+"/"+hfile.getName());
      }
      
      reader = HFile.createReader(fs, file, new CacheConfig(HBconf));
      //reader = new Reader(fs, file, null, false);

	  // Load up the index.
	  reader.loadFileInfo();
	  // Get a scanner that caches and that does not use pread.
	  scanner = reader.getScanner(false, true);
	  
	  KeyValue rowKey = KeyValue.createFirstOnRow(startr);
	  scanner.seekBefore(rowKey.getKey());
	  
	  while(scanner.next()){
		  kv = scanner.getKeyValue();
		  if(Bytes.compareTo(kv.getRow(), startr)>=0){
			  more=true;
			  return;
		  }
	  }
	  more=false;
	
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
	  boolean again=false;
	  if(!more)
		  return more;
	  if (key == null) key = new ImmutableBytesWritable();
	  if (value == null) value = new Text();
	  if(Bytes.compareTo(kv.getRow(), stopr)<=0){

		  String vars=tsplit.getVars();
		  StringTokenizer vtok = new StringTokenizer(vars);
		  int i=0;
		  while(vtok.hasMoreTokens()){
			  vtok.nextToken();
			  i++;
		  }
		  if(i==1){
			  StringTokenizer vtok2 = new StringTokenizer(vars);
			  String v1=vtok2.nextToken();
			  value.set(tsplit.getFname()+"!"+v1+"#"
					  +Bytes.toLong(kv.getQualifier())+"_");
			  //value.set(tsplit.getFname()+"sp"+v1+"$$"
				//	  +String.valueOf(Bytes.toChars(kv.getQualifier())));
			  //value.set(tsplit.getFname()+" "+v1+"$$"+valueToString(kv.getValue()));
		  }
		  else if(i==2){
			  StringTokenizer vtok2 = new StringTokenizer(vars);
			  String v1=vtok2.nextToken();
			  String v2=vtok2.nextToken();
			  byte[] r = kv.getRow();
			  byte[] r1= new byte[8];
			  for (int j = 0; j < r1.length; j++) {
				  r1[j]=r[10+j];
			  }
			  value.set(tsplit.getFname()+"!"+v1+"#"
					  +Bytes.toLong(r1)+
					  "!"+v2+"#"+Bytes.toLong(kv.getQualifier())+"_");
			  //value.set(tsplit.getFname()+"sp"+v1+"$$"
				//	  +String.valueOf(Bytes.toChars(r1))+
			  	//  "sp"+v2+"$$"+String.valueOf(Bytes.toChars(kv.getQualifier())));
			  //value.set(tsplit.getFname()+" "+v1+"$$"+valueToString(kv.getQualifier())+
			  //	  " "+v2+"$$"+valueToString(kv.getValue()));
		  }
	      more=scanner.next();
	      if(more)
			  kv = scanner.getKeyValue();
	      if(again)
	    	  return nextKeyValue();
	      return true;
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
    return 0;
  }


}
