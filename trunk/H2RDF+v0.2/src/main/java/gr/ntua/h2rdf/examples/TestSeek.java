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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestSeek {

	/**
	 * @param args
	 */
	private static Configuration hconf = HBaseConfiguration.create();
    
	public static void main(String[] args) throws NotSupportedDatatypeException {
		String address = "clone17";
		String t = "L10snappy_Index";
		String user = "npapa";
		H2RDFConf conf = new H2RDFConf(address, t, user);
		hconf=conf.getConf();
		HTable table=null, table1=null;
		String rdfs = "<http://www.w3.org/2000/01/rdf-schema#";
		String owl = "<http://www.w3.org/2002/07/owl#";
		try {
			
			table = new HTable( hconf, t );
			//Get get = new Get(Bytes.toBytes("<http://www.Department2.University10.edu/GraduateStudent69>"));
			Get get = new Get(Bytes.toBytes("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
			//Get get = new Get(Bytes.toBytes("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>"));
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
			/*get = new Get(v.getBytesWithPrefix());
			get.addColumn(Bytes.toBytes("2"), new byte[0]);
			res = table.get(get);
			System.out.println(Bytes.toString(res.value()));
			*/
			
			

			table1 = new HTable( hconf, args[0] );
			ByteTriple btr = new ByteTriple();
			btr.setPredicate(v1.getLong());
			Random ran = new Random();
			long kk=0;
			long st1 = System.currentTimeMillis();
			long st = System.currentTimeMillis();
			long sum =0;
			byte[] start = btr.getPSOByte();
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			//System.out.println(Bytes.toStringBinary(start));
			//System.out.println(Bytes.toStringBinary(stop));
			Scan scan = new Scan(start,stop);
			scan.addFamily(Bytes.toBytes("I"));
			
			scan.setAttribute("hbase.client.scanner.seekOverhead", Bytes.toBytes(Long.parseLong(args[2])));
			scan.setCaching(30000); //good for mapreduce scan
			scan.setCacheBlocks(false); //good for mapreduce scan
			scan.setBatch(30000); //good for mapreduce scan
			//scan.addColumn(Bytes.toBytes("T"), Bytes.toBytes("1"));
			ClientScanner scanner = (ClientScanner)table1.getScanner(scan);
			long last=0;
			int jump = Integer.parseInt(args[1]);
			while(true){
				Result res1 = null;
				//if(sum%10==2){
					
					if(sum%1000==0){
						double sec = ((double)System.currentTimeMillis()-st)/(double)1000;
						double thr = ((double)sum)/sec;
						System.out.println("Troughput: "+thr);
					}
					SortedBytesVLongWritable v2 = new SortedBytesVLongWritable(last+jump);
					//System.out.println("Seek to: "+v2.getLong());
					btr.setSubject(v2.getLong());
					res1 = scanner.seekTo(btr.getPSOByte(),jump);
					if(res1==null)
						break;
					/*}
				else{
					res1 = scanner.next();
					if(res1==null)
						break;
				}*/
				
				long[] n=ByteTriple.parseRow(res1.getRow());
				//System.out.println("Value: "+n[1]);
				last=n[1];
				sum++;

				/*if(sum%1000==0){
					double sec = ((double)System.currentTimeMillis()-st1)/(double)1000;
					double thr = ((double)sum)/sec;
					System.out.println(thr);
				}*/
				
			}
			double sec = ((double)System.currentTimeMillis()-st)/(double)1000;
			double thr = ((double)sum)/sec;
			System.out.println("Troughput: "+thr);
			System.out.println("Records: "+sum+" time: "+sec );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}

}
