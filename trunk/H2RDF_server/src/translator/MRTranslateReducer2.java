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
package translator;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MRTranslateReducer2 extends Reducer<Text, Text, Text, Text> {
	
	private Text outValue = new Text("");
	private Text outKey = new Text("");
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
		
		String sum = "";
		int i=0;
		for(Text v: values){
			sum+=v.toString()+"_ ";
			i++;
		}
		outKey.set(sum);
		try {
			context.write(outKey, outValue);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
 	}

	
}
