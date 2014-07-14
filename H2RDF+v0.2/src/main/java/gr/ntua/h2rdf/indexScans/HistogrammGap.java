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
package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.loadTriples.ByteTriple;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HistogrammGap {
	private long key, gap;
	private long countJoin, countOther;
	
	public HistogrammGap(long key, long gap, long countJoin, long countOther) {
		this.key = key;
		this.gap = gap;
		this.countJoin = countJoin;
		this.countOther = countOther;
	}

	public void write(Context context, long[] key, byte table, int type) {
		byte[] k = ByteTriple.createByte(key[0], key[1], key[2], table);
		String f ="";
		switch (type) {
		case 1:
			f = "A";
			break;
		case 2:
			f = "B";
			break;
		case 3:
			f = "C";
			break;

		default:
			//error 
			System.out.print("wrong length");
			System.exit(15);
			break;
		}
		SortedBytesVLongWritable s = new SortedBytesVLongWritable();
		KeyValue emmitedValue = null;
		if(!f.equals("C")){
			System.out.println("Key: "+ Bytes.toStringBinary(k)+" fam: "+f +" countJoin: "+countJoin+" countOther: "+countOther);
			emmitedValue = new KeyValue(k, Bytes.toBytes(f), (new SortedBytesVLongWritable(countJoin)).getBytesWithPrefix() 
				, (new SortedBytesVLongWritable(countOther)).getBytesWithPrefix());
		}
		else{
			System.out.println("Key: "+ Bytes.toStringBinary(k)+" fam: "+f +" countJoin: "+countJoin);
			emmitedValue = new KeyValue(k, Bytes.toBytes(f), (new SortedBytesVLongWritable(countJoin)).getBytesWithPrefix() 
					, null);
		}
			
		
    	try {
    		context.write(new ImmutableBytesWritable(k), emmitedValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
}
