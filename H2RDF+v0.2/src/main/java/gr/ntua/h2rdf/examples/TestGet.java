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

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TestGet {

	/**
	 * @param args
	 */
	private static Configuration hconf = HBaseConfiguration.create();
	public static void main(String[] args) {
		String address = "clone17";
		String t = args[0]+"_Index";
		String user = "npapa";
		H2RDFConf conf = new H2RDFConf(address, t, user);
		hconf=conf.getConf();
		HTable table=null, table1=null;
		String rdfs = "<http://www.w3.org/2000/01/rdf-schema#";
		String owl = "<http://www.w3.org/2002/07/owl#";
		
		try {
			table = new HTable( hconf, t );
			Random ran = new Random();
			long kk=0;
			long st1 = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				SortedBytesVLongWritable v11 = new SortedBytesVLongWritable(ran.nextInt(37000000));
				Get get = new Get(v11.getBytesWithPrefix());
				get.addColumn(Bytes.toBytes("2"), new byte[0]);
				Result res = table.get(get);
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
