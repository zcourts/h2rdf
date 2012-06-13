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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;

public class BufferedRecordWriter {
	private String[] varNames;
	private String[] buffer;
	private String pat;
	private int MAX_SIZE=10000;
	private ImmutableBytesWritable key = null;
	private Text value = null;
	
	public BufferedRecordWriter(String joinVars, int varsno, String fpat, String v1, String v2) {
		varNames= new String[varsno];
		buffer = new String[varsno];
		buffer[0]="";
		varNames[0]= joinVars;
		if(varNames[0].contains(v1))
			varNames[1]= v2;
		else
			varNames[1]= v1;
		pat=fpat;
	}
	
	public boolean writeValue(long value) {
		//epistrefei false otan grafei record
		if(varNames.length!=1){
			System.exit(1);
		}
		buffer[0]+=value+"_";
		flush();
		return false;
	}

	public boolean writeValue(String var1, long value1, String var2,
			long value2) {
		//epistrefei false otan grafei record
		if(varNames.length!=2){
			System.exit(1);
		}
		if(varNames[0].contains(var1)){
			if(buffer[0].length()<=1){
				buffer[0]+=value1+"_";
				buffer[1]+=value2+"_";
				return true;
			}
			else{
				if(buffer[0].contains(value1+"_")){
					buffer[0]+=value1+"_";
					buffer[1]+=value2+"_";
					if(buffer[0].length()+buffer[1].length()>=MAX_SIZE){
						flush();
						return false;
					}
					return true;
				}
				else{
					flush();
					buffer[0]+=value1+"_";
					buffer[1]+=value2+"_";
					return false;
				}
			}
		}
		else{
			if(buffer[0].length()<=1){
				buffer[0]+=value2+"_";
				buffer[1]+=value1+"_";
				return true;
			}
			else{
				if(buffer[0].contains(value2+"_")){
					buffer[0]+=value2+"_";
					buffer[1]+=value1+"_";
					if(buffer[0].length()+buffer[1].length()>=MAX_SIZE){
						flush();
						return false;
					}
					return true;
				}
				else{
					flush();
					buffer[0]+=value2+"_";
					buffer[1]+=value1+"_";
					return false;
				}
			}
		}
	}

	public void flush() {
		if (key == null) key = new ImmutableBytesWritable();
		if (value == null) value = new Text();
		String output=pat+"!";
		for (int i = 0; i < buffer.length; i++) {
			output+=varNames[i]+"#"+buffer[i]+"!";			
		}
		value.set(output);
	}

	public ImmutableBytesWritable getKey() {
		return key;
	}

	public Text getValue() {
		return value;
	}

}
