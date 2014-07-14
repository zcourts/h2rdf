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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class StatisticsCache {
	public static HashMap<String ,TreeMap<byte[],List<byte[]>>> statistics = new HashMap<String, TreeMap<byte[],List<byte[]>>>();
	
	public static SortedMap<byte[], List<byte[]>> getStatistics(HTable table, byte[] start,
			byte[] stop) throws IOException {
		String t = Bytes.toString(table.getTableName());
		TreeMap<byte[], List<byte[]>> stats = statistics.get(t);
		if(stats==null){
			stats=new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_RAWCOMPARATOR);
			statistics.put(t, stats);
		}
		
		SortedMap<byte[], List<byte[]>> res = stats.subMap(start, stop);
		/*if(res.isEmpty()){
			System.out.println("Get statistics");
			Scan scan = new Scan();
			scan.setCaching(20000); //good for mapreduce scan
			scan.setCacheBlocks(false); //good for mapreduce scan
			scan.setBatch(20000);
			//scan.setLoadColumnFamiliesOnDemand(true);
			scan.addFamily(Bytes.toBytes("S"));
			scan.addFamily(Bytes.toBytes("T"));
			scan.setStartRow(start);
			scan.setStopRow(stop);
			Iterator<Result> it = table.getScanner(scan).iterator();
			int sum=0;
			while(it.hasNext()){
				Result r = it.next();
				List<byte[]> l = res.get(r.getRow());
				if(l==null){
					l = new ArrayList<byte[]>();
					res.put(r.getRow(), l);
				}
				for(KeyValue kv : r.list()){

					sum+=1;

					byte[] b = kv.getValue();
					byte[] v = new byte[b.length+2];
					v[0] = kv.getFamily()[0];
					if(kv.getQualifier().length>0)
						v[1]=kv.getQualifier()[0];
					else
						v[1]=(byte)0;
					for (int i = 0; i < b.length; i++) {
						v[i+2] = b[i];
					}
					l.add(v);
					//if(sum%10000==0){
					//	System.out.println("Statistics: "+sum);
					//}
				}
				
				
			}
			stats.putAll(res);
		}
		System.out.println("Statistics keys:"+res.size());*/
		return res;
	}
	

	public static void initialize(HTable table) {
		String t = Bytes.toString(table.getTableName());
		System.out.println("Get statistics for table "+Bytes.toString(table.getTableName())+" ...");
		TreeMap<byte[], List<byte[]>> stats = new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_RAWCOMPARATOR);
		statistics.put(t, stats);
		try {
			Scan scan = new Scan();
			//scan.setLoadColumnFamiliesOnDemand(true);
			scan.addFamily(Bytes.toBytes("S"));
			scan.addFamily(Bytes.toBytes("T"));
			Iterator<Result> it = table.getScanner(scan).iterator();
			int sum=0;
			while(it.hasNext()){
				Result res = it.next();
				List<byte[]> l = stats.get(res.getRow());
				if(l==null){
					l = new ArrayList<byte[]>();
					stats.put(res.getRow(), l);
				}
				for(KeyValue kv : res.list()){
	
					sum+=1;
	
					byte[] b = kv.getValue();
					byte[] v = new byte[b.length+2];
					v[0] = kv.getFamily()[0];
					if(kv.getQualifier().length>0)
						v[1]=kv.getQualifier()[0];
					else
						v[1]=(byte)0;
					for (int i = 0; i < b.length; i++) {
						v[i+2] = b[i];
					}
					l.add(v);
					//if(sum%10000==0){
					//	System.out.println("Statistics: "+sum);
					//}
				}
				
				
			}
			System.out.println("Statistics: "+sum+" map size:"+statistics.size());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	
}
