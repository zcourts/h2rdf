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
package partialJoin;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class IdReducer1 extends Reducer<ImmutableBytesWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text("");
	public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException {
		
		String sum = "";
		for(Text v: values){
			sum+=v.toString()+"||" ;
		}
		outKey.set(sum);
		try {
			context.write(outKey,outValue);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	}
	
}
