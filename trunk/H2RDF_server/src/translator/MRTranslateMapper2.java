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
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRTranslateMapper2 extends Mapper<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	private String map_id;
	private Long loc_id;
	
	public void map(Text key, Text value, Context context) throws IOException {
		String val = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(val);
		StringTokenizer tok;
		while (tokenizer.hasMoreTokens()) {
			String result=tokenizer.nextToken("_");
			tok=new StringTokenizer(result);
			
			String result_id=tok.nextToken("?");
			String var = tok.nextToken();
			
			outKey.set(result_id);
			outValue.set(var+"#"+key.toString());
			try {
				context.write(outKey, outValue);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}

