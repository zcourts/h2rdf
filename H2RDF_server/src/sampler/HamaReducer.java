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
package sampler;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;

import bytes.ByteValues;

public class HamaReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {
	  
	private static final int totsize=ByteValues.totalBytes;
  	
	public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException {
		
		String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
		//byte[] id =Bytes.toBytes(Short.parseShort(idStr[idStr.length-2]));
		
		byte[] k = key.get();
		
		if(k.length!=1+3*totsize){
			System.exit(1);
		}
		byte[] newKey= new byte[1+2*totsize+2];
		for (int i = 0; i < newKey.length-2; i++) {
			newKey[i]=k[i];
		}
		
		byte[] tid = Bytes.toBytes((short)Integer.parseInt(idStr[idStr.length-2]));
		newKey[newKey.length-2]=tid[0];
		newKey[newKey.length-1]=tid[1];
		
		
		ImmutableBytesWritable emmitedKey = new ImmutableBytesWritable(
				newKey, 0, newKey.length);
		byte[] val = new byte[totsize];
		for (int i = 0; i < val.length; i++) {
			val[i]=k[1+2*totsize+i];
		}
		KeyValue emmitedValue = new KeyValue(emmitedKey.get().clone(), Bytes
				.toBytes("A"), val.clone(), null);
		try {
	    	context.write(emmitedKey, emmitedValue);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
