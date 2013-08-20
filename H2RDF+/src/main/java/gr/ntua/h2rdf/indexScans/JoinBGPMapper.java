/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinBGPMapper extends Mapper<BytesWritable, BytesWritable, ImmutableBytesWritable, Bindings> {
	
	private boolean table;
	private boolean file;
	private Scan scan;
	private byte joinVar;
	private byte pattern;

	public void map(BytesWritable key, BytesWritable value, Context context) throws IOException {
		/*int vars= ran.nextInt(100);
		Bindings b = new Bindings();
		for (int i = 0; i < vars; i++) {
			int s= ran.nextInt(100000);
			for (int j = 0; j < s; j++) {

				b.addBinding((byte)i, ran.nextLong());
			}
			System.out.println(b.map.size());
		}*/
		try {
		//	Bindings b = (Bindings)key;
			Bindings b = null;
			if(key.getClass().equals(Bindings.class)){
				b=(Bindings)key;
			}
			else{
				b = new Bindings();
				b.readFields(key.getBytes());
			}
			
			
			//if(table){
			//System.out.println(b.map);
			Set<Long> val = b.map.remove(joinVar);
			for(Long l : val){
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(l);
				byte[] k1 = v.getBytesWithPrefix();
				byte[] bytes = new byte[k1.length+2];
				bytes[0]=joinVar;
				System.arraycopy(k1, 0, bytes, 1, k1.length);
				ImmutableBytesWritable i = new ImmutableBytesWritable(bytes);
				b.pattern=pattern;
				
				context.write(i, b);
				
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		table =false;
		file = false;
		if(context.getInputSplit().getClass().equals(TableSplit.class)){
			table = true;
			scan = ((TableSplit)context.getInputSplit()).getScan();
			byte[] j = scan.getAttribute("joinVar");
			joinVar = j[0];
			j = scan.getAttribute("pattern");
			pattern = j[0];
		}
		else{
			file=true;
			String name = ((FileSplit)context.getInputSplit()).getPath().toUri().toString();
			System.out.println(name);
			name = name.split("-r-")[0];
			name+="*";
		    System.out.println("h2rdf.inputFiles_"+name+"  "+ context.getConfiguration().getInt("h2rdf.inputFiles_"+name, 0));
		    System.out.println("h2rdf.inputFilesVar_"+name+"  "+ context.getConfiguration().getInt("h2rdf.inputFilesVar_"+name, 0));
			pattern = (byte)context.getConfiguration().getInt("h2rdf.inputFiles_"+name, 0);
			joinVar = (byte)context.getConfiguration().getInt("h2rdf.inputFilesVar_"+name, 0);
			System.out.println((int)pattern+" "+(int)joinVar);
		}
	}
	
	
}
