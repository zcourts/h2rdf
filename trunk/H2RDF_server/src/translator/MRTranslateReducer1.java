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

public class MRTranslateReducer1 extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text("");
	private Text outValue = new Text("");
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
		
		String sum = "", str_id= "";
		int i=0, j=0;
		for(Text v: values){
			if(v.toString().startsWith("T")){
				str_id=v.toString().substring(1);
				i++;
			}
			else{
				sum+=v.toString()+"_";
				j++;
			}
		}
		if(i>0 && j>0){
			outKey.set(str_id);
			outValue.set(sum);
			try {
				context.write(outKey, outValue);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
 	}

	
}
