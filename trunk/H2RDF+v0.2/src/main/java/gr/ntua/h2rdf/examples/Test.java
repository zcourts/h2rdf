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
package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;
import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.loadTriples.ByteTriple;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {

	/**
	 * @param args
	 */
	private static Configuration hconf = HBaseConfiguration.create();
    
	public static void main(String[] args) throws NotSupportedDatatypeException {
		/*LinkedList<Long> geomean = new LinkedList<Long>();
		Random r = new Random();
		for (int i = 0; i < 150; i++) {
			long l = 1250+r.nextInt(50);
			
			if(geomean.size()<7)
				geomean.addLast((l-50));
			else{
				geomean.removeFirst();
				geomean.addLast((l-50));
			}
			double mult =1;
			for(Long l1 : geomean){
				mult*=(double)l1;
			}
			System.out.println(geomean);
			double p = (double)1.0/(double)geomean.size();
			//System.out.println(p);
			double gm = Math.pow(mult, p);//(double)sum/(double)geomean.size();
			//double gm =(ExecutorOpenRdf.execTime-50);
			System.out.println(+gm);
		}
		System.exit(0);*/
		
		
		String address = "master";
		String t = "L10_Index";
		String user = "root";
		H2RDFConf conf = new H2RDFConf(address, t, user);
		hconf=conf.getConf();
		HTable table=null, table1=null;
		String rdfs = "<http://www.w3.org/2000/01/rdf-schema#";
		String owl = "<http://www.w3.org/2002/07/owl#";
		try {
			
			table = new HTable( hconf, t );
			//Get get = new Get(Bytes.toBytes("<http://www.Department2.University10.edu/GraduateStudent69>"));
			//Get get = new Get(Bytes.toBytes("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
			Get get = new Get(Bytes.toBytes("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>"));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = table.get(get);
			
			SortedBytesVLongWritable v1 = new SortedBytesVLongWritable();
			v1.setBytesWithPrefix(res.value());

			System.out.println(v1.getLong());
			System.out.println(Bytes.toStringBinary(v1.getBytesWithPrefix()));

			/*Random ran = new Random();
			long kk=0;
			long st1 = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				SortedBytesVLongWritable v11 = new SortedBytesVLongWritable(ran.nextInt(37000000));
				get = new Get(v11.getBytesWithPrefix());
				get.addColumn(Bytes.toBytes("2"), new byte[0]);
				res = table.get(get);
				//System.out.println(Bytes.toString(res.value()));
				kk++;
				
				if(kk%100==0){
					double sec = ((double)System.currentTimeMillis()-st1)/(double)1000;
					double thr = ((double)kk)/sec;
					System.out.println(thr);
				}
			}
			double sec1 = ((double)System.currentTimeMillis()-st1)/(double)1000;
			double thr1 = ((double)kk)/sec1;
			System.out.println("Troughput: "+thr1);
			System.out.println("Requests: "+kk+" time: "+sec1 );
			System.exit(0);
			*/
			get = new Get(Bytes.toBytes("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent>"));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			res = table.get(get);
			SortedBytesVLongWritable v2 = new SortedBytesVLongWritable();
			v2.setBytesWithPrefix(res.value());
			System.out.println(v2.getLong());
			
			System.out.println(Bytes.toStringBinary(v2.getBytesWithPrefix()));

			/*get = new Get(v.getBytesWithPrefix());
			get.addColumn(Bytes.toBytes("2"), new byte[0]);
			res = table.get(get);
			System.out.println(Bytes.toString(res.value()));
			*/
			
			

			/*table1 = new HTable( hconf, args[0] );
			int threadnum = Integer.parseInt(args[1]);
			ThreadScan[] thread = new ThreadScan[threadnum];
			long start1 = Long.parseLong(args[2]);
			long chunk = 195442429/threadnum;
			for (int threadId = 0; threadId < threadnum; threadId++) {
				thread[threadId] = new ThreadScan(table1, start1, start1+chunk, v1.getLong());
				thread[threadId].start();
				start1 +=chunk;
			}
			for (int threadId = 0; threadId < threadnum; threadId++) {
		    	  try {
		            thread[threadId].join();
		         }
		         catch (InterruptedException e) {
		            System.out.print("Scan interrupted\n");
		         }
		      }
		      
		      System.out.print("Scan completed\n");
			System.exit(1);*/
			table1 = new HTable( hconf, "L10" );
			ByteTriple btr = new ByteTriple();
			btr.setPredicate(v1.getLong());
			//btr.setSubject(v2.getLong());
			byte[] start = btr.getPSOByte();
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			System.out.println(Bytes.toStringBinary(start));
			System.out.println(Bytes.toStringBinary(stop));
			Scan scan = new Scan(start,stop);
			scan.addFamily(Bytes.toBytes("I"));
			
			scan.setCaching(30000); //good for mapreduce scan
			scan.setCacheBlocks(false); //good for mapreduce scan
			scan.setBatch(30000); //good for mapreduce scan
			//scan.addColumn(Bytes.toBytes("T"), Bytes.toBytes("1"));
			Iterator<Result> it = table1.getScanner(scan).iterator();
			long sum=0, min=0,max=0;
			long st = System.currentTimeMillis();
			long[] n=null;
			while(it.hasNext()){
				res = it.next();
				if(res.size()>0){
					/*SortedBytesVLongWritable v = new SortedBytesVLongWritable();
					v.setBytesWithPrefix(res.value());
					sum += v.getLong();
					*/
					//System.out.println(v.getLong());
					
					//byte[] temp = res.getRow();
					//System.out.println(Bytes.toStringBinary(temp));
					//n = ByteTriple.parseRow(temp);
					/*if(sum==0){
						min=n[1];
					}
					String[] r = new String[3];
					SortedBytesVLongWritable v = new SortedBytesVLongWritable(n[0]);
					get = new Get(v.getBytesWithPrefix());
					get.addColumn(Bytes.toBytes("2"), new byte[0]);
					res = table.get(get);
					r[0] = Bytes.toString(res.value());

					v = new SortedBytesVLongWritable(n[1]);
					get = new Get(v.getBytesWithPrefix());
					get.addColumn(Bytes.toBytes("2"), new byte[0]);
					res = table.get(get);
					r[1] = Bytes.toString(res.value());
					
					v = new SortedBytesVLongWritable(n[2]);
					get = new Get(v.getBytesWithPrefix());
					get.addColumn(Bytes.toBytes("2"), new byte[0]);
					res = table.get(get);
					r[2] = Bytes.toString(res.value());
					System.out.println(r[0]+" "+r[1]+" "+r[2]);*/
					if(res.size()>1){
						for(KeyValue e : res.list()){
							byte[] temp = e.getRow();
							n = ByteTriple.parseRow(temp);
							System.out.println(n[0]+" "+n[1]+" "+n[2]);
							//System.exit(0);
						}
					}
					
					sum++;
					
					if(sum%10000==0){
						double sec = ((double)System.currentTimeMillis()-st)/(double)1000;
						double thr = ((double)sum)/sec;
						System.out.println(thr);
					}
				}
			}
			double sec = ((double)System.currentTimeMillis()-st)/(double)1000;
			double thr = ((double)sum)/sec;
			System.out.println("Min: "+min+" max: "+n[1]);
			System.out.println("Troughput: "+thr);
			System.out.println("Records: "+sum+" time: "+sec );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}

}
