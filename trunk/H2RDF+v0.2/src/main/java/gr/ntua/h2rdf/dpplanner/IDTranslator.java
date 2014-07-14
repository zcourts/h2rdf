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
package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class IDTranslator {
	private static HashMap<String,HashMap<String, Long>> idCache= new HashMap<String, HashMap<String,Long>>();
	
	public static Long translate(String name, HTable indexTable) throws IOException{
		HashMap<String, Long> tablemap = idCache.get(Bytes.toString(indexTable.getTableName()));
		if(tablemap!=null){
			Long l = tablemap.get(name);
			if(l!=null)
				return l;
			else{
				Get get = new Get(Bytes.toBytes(name));
				get.addColumn(Bytes.toBytes("1"), new byte[0]);
				Result res = indexTable.get(get);
				SortedBytesVLongWritable v = new SortedBytesVLongWritable();
				if(res.isEmpty())
					throw new IOException("node:"+name+" not found");
				v.setBytesWithPrefix(res.value());
				tablemap.put(name, v.getLong());
				return v.getLong();
			}
		}
		else{
			tablemap = new HashMap<String, Long>();
			idCache.put(Bytes.toString(indexTable.getTableName()), tablemap);
			Get get = new Get(Bytes.toBytes(name));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			if(res.isEmpty())
				throw new IOException("node:"+name+" not found");
			v.setBytesWithPrefix(res.value());
			tablemap.put(name, v.getLong());
			return v.getLong();
		}
	}
}
