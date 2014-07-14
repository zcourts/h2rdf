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
package gr.ntua.h2rdf.loadTriples;

import gr.ntua.h2rdf.indexScans.IndexHistogramm;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HexaStoreHistogramsReduce extends Reducer<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, KeyValue> {
  	private long[] countJoin, prev;
  	private IndexHistogramm hist1, hist2, hist3;
  	private boolean first;
  	private byte[] lastkey;
  	private long[] lastK;
  	
	public void reduce(ImmutableBytesWritable key, Iterable<NullWritable> values, Context context) throws IOException {
		long[] n = ByteTriple.parseRow(key.get());
		System.out.println("Key: "+n[0]+" "+n[1]+" "+n[2] 
				+" bytes:"+Bytes.toStringBinary(key.get()));
		lastkey = key.get();
		lastK = n;
		if(first){
			first=false;
			countJoin[0]=1;
			countJoin[1]=1;
			countJoin[2]=1;
			
			prev[0]=n[0];
			prev[1]=n[1];
			prev[2]=n[2];
			
			hist1 = new IndexHistogramm(key.get()[0], n, n[0],1);
			hist2 = new IndexHistogramm(key.get()[0], n, n[1],2);
			hist3 = new IndexHistogramm(key.get()[0], n, n[2],3);
			
		}
		
		if(n[0]==prev[0]){
			if(n[1] == prev[1]){
				if(n[2] == prev[2]){//same all
					//ERROR
					System.out.println("same key");
				}
				else{//same first same second new third
					hist3.addKey(n[2], 1);
					countJoin[1]++;
				}
			}
			else{// same first new second new third
				hist3.addLastCount(1);
				hist3.write(context);
				hist3 = new IndexHistogramm(key.get()[0], n, n[2],3);
				
				hist2.addKey(n[1], countJoin[1]);
				
				countJoin[0] += countJoin[1];
				countJoin[1] = 1;
			}
		}
		else{//new first new second new third
			hist3.addLastCount(1);
			hist3.write(context);
			hist3 = new IndexHistogramm(key.get()[0], n, n[2],3);
			
			hist2.addLastCount(countJoin[1]);
			hist2.write(context);
			hist2 = new IndexHistogramm(key.get()[0], n, n[1],2);

			countJoin[0] += countJoin[1];
			hist1.addKey(n[0], countJoin[0]);
			countJoin[0] = 1;
			countJoin[1] = 1;
			
		}
		
		KeyValue emmitedValue = new KeyValue(key.get().clone(), Bytes.toBytes("I"), null , null);
		try {
	    	context.write(key, emmitedValue);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		if(first){
			super.cleanup(context);
			return;
		}
		hist3.addLastCount(1);
		hist3.write(context);
		hist3 = new IndexHistogramm(lastkey[0], lastK, lastK[2],3);
		
		hist2.addLastCount(countJoin[1]);
		hist2.write(context);
		hist2 = new IndexHistogramm(lastkey[0], lastK, lastK[1],2);

		hist1.addLastCount(countJoin[0]);
		hist1.write(context);
		hist1 = new IndexHistogramm(lastkey[0], lastK, lastK[1],1);
		
		
		super.cleanup(context);
	}
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		countJoin = new long[3];
		prev = new long[3];
		first = true;
	}
	
}
