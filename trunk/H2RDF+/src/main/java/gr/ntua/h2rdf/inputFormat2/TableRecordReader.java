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
import gr.ntua.h2rdf.loadTriples.ByteTriple;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
public class TableRecordReader
extends RecordReader<Bindings, BytesWritable> {
  private int numBound;
  private Bindings current, next;
  private TableRecordReaderImpl recordReaderImpl = new TableRecordReaderImpl();
private byte[] vars;
private long nextJoinVar;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow  The first row to start at.
   * @throws IOException When restarting fails.
   */
  public void restart(byte[] firstRow) throws IOException {
    this.recordReaderImpl.restart(firstRow);
  }


  /**
   * Sets the HBase table.
   *
   * @param htable  The {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    this.recordReaderImpl.setHTable(htable);
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan  The scan to set.
   */
  public void setScan(Scan scan) {
	  scan.setCaching(30000); //good for mapreduce scan
	  scan.setCacheBlocks(false); //good for mapreduce scan
	  scan.setBatch(30000); //good for mapreduce scan
	  current = new Bindings();
	  next = new Bindings();
	  vars = scan.getFamilies()[0];
	  numBound = 3-(vars.length);
	  scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
	  scan.addFamily(Bytes.toBytes("I"));
	  
	  this.recordReaderImpl.setScan(scan);
  }

  /**
   * Closes the split.
   *
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() {
    this.recordReaderImpl.close();
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
  public Bindings getCurrentKey() throws IOException,
      InterruptedException {
    return current;
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
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return new BytesWritable();
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
    this.recordReaderImpl.initialize(inputsplit, context);
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
	  current = next;
	  next = new Bindings();
	  long joinVar;
	  if(current.map.keySet().isEmpty()){
		  if(recordReaderImpl.nextKeyValue()){
			  Result r = recordReaderImpl.getCurrentValue();
			  byte[] temp = r.getRow();
			  long[] n = ByteTriple.parseRow(temp);
			  for (int i = 0; i < vars.length; i++) {
				  current.addBinding(vars[i], n[numBound+i]);
			  }
			  joinVar = n[numBound];
		  }
		  else{
			  return false;
		  }
	  }
	  else{
		  joinVar= nextJoinVar;
	  }
	  while(recordReaderImpl.nextKeyValue()){
		  Result r = recordReaderImpl.getCurrentValue();
		  byte[] temp = r.getRow();
		  long[] n = ByteTriple.parseRow(temp);
		  if(joinVar == n[numBound]){
			  for (int i = 0; i < vars.length; i++) {
				  current.addBinding(vars[i], n[numBound+i]);
			  }
		  }
		  else{
			  for (int i = 0; i < vars.length; i++) {
				  next.addBinding(vars[i], n[numBound+i]);
			  }
			  nextJoinVar = n[numBound];
			  return true;
		  }
	  }
	  return !current.map.isEmpty();
  }
  
  /**
   * The current progress of the record reader through its data.
   *
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    return this.recordReaderImpl.getProgress();
  }
}
