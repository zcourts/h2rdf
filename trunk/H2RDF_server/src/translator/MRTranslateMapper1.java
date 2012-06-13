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

public class MRTranslateMapper1 extends Mapper<ImmutableBytesWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	private String map_id;
	private Long loc_id;
	
	public void map(ImmutableBytesWritable key, Text value, Context context) throws IOException {
		
    	
		String line = value.toString();
		if(line.startsWith("T")){
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			tokenizer.nextToken("!");
			outKey.set(tokenizer.nextToken("$$").substring(1));
			outValue.set("T"+tokenizer.nextToken("$$"));
			try {
				context.write(outKey, outValue);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		else{
			StringTokenizer tokenizer = new StringTokenizer(line);
			StringTokenizer tok;
			while (tokenizer.hasMoreTokens()) {
				String binding=tokenizer.nextToken("!");
				if(binding.startsWith("?")){
					tok=new StringTokenizer(binding);
					String var=tok.nextToken("#");
					if(!tok.hasMoreTokens()){
						return;
					}
					String b = tok.nextToken("#");
					
					StringTokenizer bintok = new StringTokenizer(b);
					while(bintok.hasMoreTokens()) {
						String temp1 = bintok.nextToken("_");
						outKey.set(temp1);
						outValue.set(map_id+"#"+loc_id+var);
						try {
							context.write(outKey, outValue);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
				}
			}
		}
		loc_id++;
		
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		String[] idStr = context.getConfiguration().get("mapred.task.id").split("_");
		map_id = Short.parseShort(idStr[idStr.length-2])+"";
		loc_id= new Long(0);
	}

	
}

