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

import gr.ntua.h2rdf.coprocessors.ThreadedProxy;
import gr.ntua.h2rdf.coprocessors.TranslateIndexProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

public class IndexTranslator {
	HTable table;
	
	public IndexTranslator(String table) throws IOException {
		this.table = new HTable(HBaseConfiguration.create(), table);
	}

	public HashMap<String, Long> translate(TreeSet<String> set) {
	    try {
	    	
		    Iterator<String> itreader = set.iterator();
		    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null ||
			        keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}

			byte[] first = null;
			if(itreader.hasNext()){
				first = Bytes.toBytes(itreader.next());
			}
			else{
				throw new IOException("Expecting at least one key.");
			}
			List<List<byte[]>> lists = new ArrayList<List<byte[]>>();
			List<byte[]> regions = new ArrayList<byte[]>();
			for (int i = 0; i < keys.getFirst().length; i++) {
				int comp1 = Bytes.compareTo(first, keys.getSecond()[i]);
				if(comp1<0){//used region
					List<byte[]> list = new ArrayList<byte[]>();
					list.add(first);
					boolean hasNext= false;
					while(hasNext=itreader.hasNext()){
						byte[] temp=Bytes.toBytes(itreader.next());
						int comp = Bytes.compareTo(temp, keys.getSecond()[i]);
						if(comp<0){
							list.add(temp);
						}
						else{
							if(list.size()>0){//send translate request
								lists.add(list);
								regions.add(keys.getFirst()[i]);
							}
							first=temp;
							break;
						}
					}
					if(!hasNext){
						if(list.size()>0){//send translate request
							lists.add(list);
							regions.add(keys.getFirst()[i]);
						}
						break;
					}
				}
				
			}

			set.clear();
			
			int threads = lists.size();
			ThreadedProxy[] thread= new ThreadedProxy[threads];

			HashMap <String, Long>[] index = new HashMap[threads];
			for (int i = 0; i < threads; i++) {
				index[i] = new HashMap<String, Long>();
			}
			
			
			//HashMap <String, Long> ind = new HashMap<String, Long>();
			Random ran = new Random();
			int s = ran.nextInt(threads);
			System.out.println("threads: "+threads);
			int chunk=20;

		    ExecutorService executor = Executors.newFixedThreadPool(chunk);
			for (int threadId = s; threadId < threads; threadId++) {
				ThreadedProxy worker = new ThreadedProxy(index[threadId], table, lists.get(threadId), regions.get(threadId));
				executor.execute(worker);
			}

			for (int threadId = s-1; threadId >=0; threadId--) {
				ThreadedProxy worker = new ThreadedProxy(index[threadId], table, lists.get(threadId), regions.get(threadId));
				executor.execute(worker);
				
				//translate(fullIndex, lists.get(threadId), regions.get(threadId));
			}
			executor.shutdown();
		    // Wait until all threads are finish
		    while (!executor.isTerminated()) {

		    }
			/*for (int threadId = 0; threadId < threads; threadId++) {
				thread[threadId] = new ThreadedProxy(index[threadId], table, lists.get(threadId), regions.get(threadId));
				thread[threadId].start();
			}
		    for (int i=0; i <threads; i++) {
		    	try {
		    		thread[i].join();
		    	}
		        catch (InterruptedException e) {
		        	System.out.print("Join interrupted\n");
		        }
		    }
			*/
		    int count =0;
			for (int i = 0; i < threads; i++) {
				count+=index[i].size();
			}
		    
			HashMap <String, Long> fullIndex = new HashMap <String, Long>(count);
			for (int i = 0; i < threads; i++) {
				fullIndex.putAll(index[i]);
				index[i].clear();
			}
			
			return fullIndex;
			

	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
		return null;
		
		
	}

	private void translate(HashMap<String, Long> index, List<byte[]> list, byte[] row){
		TranslateIndexProtocol pr = table.coprocessorProxy(TranslateIndexProtocol.class, row);
	    List<byte[]> ret;
		try {
			ret = pr.translate(list);
			
		    Iterator<byte[]> itkey = list.iterator();
		    
		    if(ret==null){
		    	while(itkey.hasNext()){
			    	System.out.println(Bytes.toString(itkey.next()));
		    	}
		    }
		    
		    Iterator<byte[]> itvalue = ret.iterator();
		    while(itkey.hasNext()){
		    	SortedBytesVLongWritable v = new SortedBytesVLongWritable();
		    	byte[] temp = itvalue.next();
		    	if(temp.length==0){
		    		//System.out.println(Bytes.toString(itkey.next()));
		    		continue;
		    	}
		    	v.setBytesWithPrefix(temp);
		    	//System.out.println(Bytes.toString(itkey.next())+"\t"+v.getLong());
		    	index.put(Bytes.toString(itkey.next()), v.getLong());
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
