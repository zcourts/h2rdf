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
package gr.ntua.h2rdf.client;

import gr.ntua.h2rdf.indexScans.Bindings;
import gr.ntua.h2rdf.indexScans.Combinations;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.query.impl.MapBindingSet;

public class BindingsResultIterable {
	private Combinations c;
	private boolean hasNext;
	private List<Long> currentVal; 
	private ArrayList<Byte> variables;
	private HTable table;
	private HashMap<Long,MyValue> cacheIndex;
	private HashMap<Integer, String> varIds;
	
	public BindingsResultIterable(Bindings bindings, HTable table, HashMap<Integer, String> varIds) throws IOException {
		//System.out.println(bindings.map);
		this.varIds=varIds;
		this.table=table;
		cacheIndex = new HashMap<Long, MyValue>();
		variables = new ArrayList<Byte>();
		variables.addAll(bindings.map.keySet());
		c = new Combinations(bindings, variables);
		currentVal = c.nextBinding();
		hasNext = (currentVal!=null);
	}
	
	public boolean hasNext() {
		return hasNext;
	}

	public MapBindingSet next() throws IOException {
		List<Long> temp = currentVal;
		currentVal = c.nextBinding();
		hasNext = (currentVal!=null);
		MapBindingSet ret = new MapBindingSet();
		int i =0;
		for(Byte b : variables){
			ret.addBinding(varIds.get(b.intValue()), translate(temp.get(i)));
			i++;
		}
		return ret;
	}
	
	
	
	private MyValue translate(Long l) throws IOException {
		MyValue s = cacheIndex.get(l);
		if(s!=null)
			return s;
		
		SortedBytesVLongWritable v = new SortedBytesVLongWritable(l);
		Get get = new Get(v.getBytesWithPrefix());
		get.addColumn(Bytes.toBytes("2"), new byte[0]);
		Result res = table.get(get);
		if(res.isEmpty())
			throw new IOException("node not found");
		MyValue r = new MyValue(Bytes.toString(res.value()));
		cacheIndex.put(l, r);
		return r;
	}

	public static void main(String[] args) {
		Bindings b = new Bindings();
		
		Set<Long> l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)1, l);
		
		l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)2, l);
		
		l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)3, l);
		
		l = new TreeSet<Long>();
		l.add(new Long(1));
		l.add(new Long(2));
		l.add(new Long(3));
		l.add(new Long(4));
		b.map.put((byte)4, l);
		
		BindingsResultIterable it;
		try {
			Configuration conf = HBaseConfiguration.create();
			HTable t = new HTable(conf, "L10_Index");
			it = new BindingsResultIterable(b, t,null);
			while(it.hasNext()){
				it.next();
				System.out.println(it.next());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
