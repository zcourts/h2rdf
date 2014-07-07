/*******************************************************************************
 * Copyright 2014 Nikolaos Papailiou
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.inputFormat2;

import java.io.IOException;
import java.util.List;

import gr.ntua.h2rdf.indexScans.Bindings;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordReader;

public interface TableRecordReaderInt {
	public List<Bindings> getCurrentKey();
	public BytesWritable getCurrentValue();
	public boolean nextKeyValue() throws IOException, InterruptedException;
	public boolean goTo(long k) throws IOException, InterruptedException;
	public void close() throws IOException;
	public Long getJvar();
}
