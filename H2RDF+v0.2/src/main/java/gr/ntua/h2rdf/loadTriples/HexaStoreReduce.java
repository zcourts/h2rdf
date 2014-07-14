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

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class HexaStoreReduce extends Reducer<ImmutableBytesWritable, NullWritable, ImmutableBytesWritable, KeyValue> {
	private byte[] prev2firstByte, prevfirstByte;
	private ImmutableBytesWritable lastKey;
	private long[] prev2first;
	private long[] prevfirst;
	private int first;
	private static final int statisticsOffset =50;
  	
	public void reduce(ImmutableBytesWritable key, Iterable<NullWritable> values, Context context) throws IOException {
		//String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
		//byte[] id =Bytes.toBytes(Short.parseShort(idStr[idStr.length-2]));
		byte[] temp = key.get();
		//System.out.println("key: "+Bytes.toStringBinary(temp));
		long[] n = ByteTriple.parseRow(temp);
		
		if(prev2first==null && prevfirst==null ){
			prev2firstByte = key.get().clone();//ByteTriple.createByte(n[0], n[1], temp[0]);
			prev2first = new long[3];
			prev2first[0]=n[0];
			prev2first[1]=n[1];
			prev2first[2]=0; //count

			prevfirstByte = key.get().clone();//ByteTriple.createByte(n[0], temp[0]);
			prevfirst = new long[3];
			prevfirst[0]=n[0];
			prevfirst[1]=0; //first count
			prevfirst[2]=0; //total count
			//first = 1;
			//firstKey = new ImmutableBytesWritable(key.get());
		}

		//System.out.println("prevfirstByte: "+Bytes.toStringBinary(prevfirstByte)+" \t prev2firstByte: "+Bytes.toStringBinary(prev2firstByte));
		if(n[0]==prev2first[0]){//same first
			if(n[1] == prev2first[1]){//same second
				prev2first[2]++;
			}
			else{//new second
				prevfirst[1]++;
				prevfirst[2]+=prev2first[2];
				
				//System.out.println("adding: "+Bytes.toStringBinary(prev2firstByte));
				if(prev2first[2]>=statisticsOffset){
					KeyValue emmitedValue = new KeyValue(prev2firstByte, Bytes.toBytes("S"), null ,(new SortedBytesVLongWritable(prev2first[2])).getBytesWithPrefix());
					try {
				    	context.write(new ImmutableBytesWritable(prev2firstByte), emmitedValue);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				prev2firstByte = ByteTriple.createByte(n[0], n[1], temp[0]);
				prev2first[0]=n[0];
				prev2first[1]=n[1];
				prev2first[2]=1; 
			}
		}
		else{ //new first
			prevfirst[1]++;
			prevfirst[2]+=prev2first[2];

			if(prev2first[2]>=statisticsOffset){
				//System.out.println("adding: "+Bytes.toStringBinary(prev2firstByte));
				KeyValue emmitedValue = new KeyValue(prev2firstByte, Bytes.toBytes("S"), null ,(new SortedBytesVLongWritable(prev2first[2])).getBytesWithPrefix());
				try {
			    	context.write(new ImmutableBytesWritable(prev2firstByte), emmitedValue);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			prev2firstByte = ByteTriple.createByte(n[0], n[1], temp[0]);
			prev2first[0]=n[0];
			prev2first[1]=n[1];
			prev2first[2]=1; 
			
			//System.out.println("adding: "+Bytes.toStringBinary(prevfirstByte));
			if(prevfirst[1]>=statisticsOffset || prevfirst[2]>=statisticsOffset){
				KeyValue emmitedValue1 = new KeyValue(prevfirstByte, Bytes.toBytes("T"), Bytes.toBytes("1") ,(new SortedBytesVLongWritable(prevfirst[1])).getBytesWithPrefix());
				try {
			    	context.write(new ImmutableBytesWritable(prevfirstByte), emmitedValue1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				//System.out.println("adding: "+Bytes.toStringBinary(prevfirstByte));
				KeyValue emmitedValue2 = new KeyValue(prevfirstByte, Bytes.toBytes("T"), Bytes.toBytes("2") ,(new SortedBytesVLongWritable(prevfirst[2])).getBytesWithPrefix());
				try {
			    	context.write(new ImmutableBytesWritable(prevfirstByte), emmitedValue2);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			prevfirstByte = ByteTriple.createByte(n[0], temp[0]);
			prevfirst[0]=n[0];
			prevfirst[1]=1;
			prevfirst[2]=0; 
		}

		lastKey = key;
		KeyValue emmitedValue = new KeyValue(key.get().clone(), Bytes.toBytes("I"), null , null);
		try {
	    	context.write(key, emmitedValue);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {

		if(prev2first==null || prevfirst==null ){
			super.cleanup(context);
			return;
		}
			
		prevfirst[1]++;
		prevfirst[2]+=prev2first[2];

		if(prev2first[2]>=statisticsOffset){
			//System.out.println("adding: "+Bytes.toStringBinary(prev2firstByte));
			KeyValue emmitedValue = new KeyValue(lastKey.get(), Bytes.toBytes("S"), null ,(new SortedBytesVLongWritable(prev2first[2])).getBytesWithPrefix());
			try {
		    	context.write(lastKey, emmitedValue);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//System.out.println("adding: "+Bytes.toStringBinary(prevfirstByte));
		if(prevfirst[1]>=statisticsOffset || prevfirst[2]>=statisticsOffset){
			KeyValue emmitedValue1 = new KeyValue(lastKey.get(), Bytes.toBytes("T"), Bytes.toBytes("1") ,(new SortedBytesVLongWritable(prevfirst[1])).getBytesWithPrefix());
			try {
		    	context.write(lastKey, emmitedValue1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			//System.out.println("adding: "+Bytes.toStringBinary(prevfirstByte));
			KeyValue emmitedValue2 = new KeyValue(lastKey.get(), Bytes.toBytes("T"), Bytes.toBytes("2") ,(new SortedBytesVLongWritable(prevfirst[2])).getBytesWithPrefix());
			try {
		    	context.write(lastKey, emmitedValue2);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		super.cleanup(context);
	}

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	  	first=0;
	  	prev2first=null;
	  	prevfirst=null;
	  	lastKey = null;
	}
}
