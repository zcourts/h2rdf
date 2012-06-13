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
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobContext)} and passed to
 * {@link InputFormat#createRecordReader(InputSplit,TaskAttemptContext)}. */
public class MyFileSplit extends MyInputSplit implements Writable {
  private Path file;
  private long start;
  private long length;
  private String[] hosts;

  MyFileSplit() {super.type=0;}

  /** Constructs a split with host information
   *
   * @param file the file name
   * @param start the position of the first byte in the file to process
   * @param length the number of bytes in the file to process
   * @param hosts the list of hosts containing the block, possibly null
   */
  public MyFileSplit(Path file, long start, long length, String[] hosts) {
	super.type=0;
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
  }
 
  /** The file containing this split's data. */
  public Path getPath() { return file; }
  
  /** The position of the first byte in the file to process. */
  public long getStart() { return start; }
  
  /** The number of bytes in the file to process. */
  @Override
  public long getLength() { return length; }

  @Override
  public String toString() { return file + ":" + start + "+" + length; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
  }

  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    hosts = null;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }
}
