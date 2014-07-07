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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;

import gr.ntua.h2rdf.bytes.ByteValues;
import gr.ntua.h2rdf.bytes.H2RDFNode;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.graph.Triple;

public class HbaseSequentialLoader implements Loader {
	private HTable table, statistics;
	private H2RDFConf conf;
	private final int totsize=ByteValues.totalBytes, rowlength=1+2*totsize;

	public HbaseSequentialLoader(H2RDFConf conf) {
		this.conf=conf;
		String t =conf.getTable();
		Configuration hbconf= conf.getConf();
		try {
			//HBaseConfiguration.create();
			//hbconf.set("hbase.zookeeper.quorum", "ia200124.eu.archive.org");
			//hbconf.set("hbase.zookeeper.property.clientPort", "2222");
			//System.out.println(hbconf.get("hbase.rootdir"));
			//System.out.println(hbconf.get("hbase.zookeeper.quorum"));
			//System.out.println(hbconf.get("hbase.zookeeper.property.clientPort"));
			//System.out.println("ok");
			HBaseAdmin hadmin = new HBaseAdmin(hbconf);
			System.out.println(conf.getAddress());
			System.out.println(t);
			if(!hadmin.tableExists(t+"")){
				System.out.println("creating "+t);
				HTableDescriptor desc = new HTableDescriptor(t);
				HColumnDescriptor family= new HColumnDescriptor("A");
				desc.addFamily(family); 
				try {
					hadmin.createTable(desc);
				} catch (TableExistsException e) {
					table = new HTable(hbconf, t);
				}
			}

			if(!hadmin.tableExists(t+"_stats")){
				System.out.println("creating "+t+"_stats");
				HTableDescriptor desc = new HTableDescriptor(t+"_stats");
				HColumnDescriptor family= new HColumnDescriptor("size");
				desc.addFamily(family); 
				try {
					hadmin.createTable(desc);
				} catch (TableExistsException e) {
					statistics = new HTable(hbconf, t+"_stats");
				}
			}
			table = new HTable(hbconf, t);
			statistics = new HTable(hbconf, t+"_stats");
		}catch (IOException e) {
			try {
				table = new HTable(hbconf, t);
				statistics = new HTable(hbconf, t+"_stats");
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
	}

	@Override
	public void add(Triple triple) throws NotSupportedDatatypeException {
		H2RDFNode nodeS = new H2RDFNode(triple.getSubject());
		H2RDFNode nodeP = new H2RDFNode(triple.getPredicate());
		H2RDFNode nodeO = new H2RDFNode(triple.getObject());
    	byte[] si = nodeS.getHashValue();
    	byte[] pi = nodeP.getHashValue();
    	byte[] oi = nodeO.getHashValue();
    	List<Put> list = new  LinkedList<Put>();
		
    	//reverse hash values
    	byte[] s = nodeS.getStringValue();
    	byte[] p = nodeP.getStringValue();
    	byte[] o = nodeO.getStringValue();
    	byte[] row = new byte[totsize+1];
    	byte[] qual = new byte[s.length];
    	Put put;
    	if(s!= null){
	    	row[0] =(byte)1;
	    	for (int i = 0; i < totsize; i++) {
	    		row[i+1]=si[i];
			}
	    	for (int i = 0; i < s.length; i++) {
	    		qual[i]=s[i];
	    	}
	    	put =new Put(row);
			put.add(Bytes.toBytes("A"),Bytes.toBytes("i"), qual);
			
			list.add(put);
    	}
    	if(p!=null){
			row = new byte[totsize+1];
	    	qual = new byte[p.length];
	    	row[0] =(byte)1;
	    	for (int i = 0; i < totsize; i++) {
	    		row[i+1]=pi[i];
			}
	    	for (int i = 0; i < p.length; i++) {
	    		qual[i]=p[i];
	    	}
	    	put =new Put(row);
			put.add(Bytes.toBytes("A"), Bytes.toBytes("i"), qual);
			list.add(put);
    	}

    	if(o!=null){
			row = new byte[totsize+1];
	    	qual = new byte[o.length];
	    	row[0] =(byte)1;
	    	for (int i = 0; i < totsize; i++) {
	    		row[i+1]=oi[i];
			}
	    	for (int i = 0; i < o.length; i++) {
	    		qual[i]=o[i];
	    	}
	    	put =new Put(row);
			put.add(Bytes.toBytes("A"), Bytes.toBytes("i"), qual);
			
			list.add(put);
    	}
		//dhmiourgia spo byte[0]=4 emit row=si,pi col=oi
		row = new byte[rowlength+2];
		byte[] statrow= new byte[totsize+1];
		byte[] statrowfull= new byte[rowlength-2];
		qual = new byte[totsize];
		row[0] =	(byte)4;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=si[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		row[i+totsize+1]=pi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		qual[i]=oi[i];
		}
		row[rowlength] =(byte)1;
		row[rowlength+1] =(byte)1;
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);
    	try {
    		for (int i = 0; i < statrow.length; i++) {
				statrow[i]=row[i];
			}
			statistics.incrementColumnValue(statrow, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(1));
    		for (int i = 0; i < statrowfull.length; i++) {
    			statrowfull[i]=row[i];
			}
			statistics.incrementColumnValue(statrowfull, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(1));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		//dhmiourgia pos byte[0]=3 emit row=pi,oi col=si
		row = new byte[rowlength+2];
		qual = new byte[totsize];
		row[0] =	(byte)3;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=pi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		row[i+totsize+1]=oi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		qual[i]=si[i];
		}
		row[rowlength] =(byte)1;
		row[rowlength+1] =(byte)1;
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);
    	try {
    		for (int i = 0; i < statrow.length; i++) {
				statrow[i]=row[i];
			}
			statistics.incrementColumnValue(statrow, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(1));
    		for (int i = 0; i < statrowfull.length; i++) {
    			statrowfull[i]=row[i];
			}
			statistics.incrementColumnValue(statrowfull, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(1));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
		//dhmiourgia osp byte[0]=2 emit row=oi,si col=pi
		row = new byte[rowlength+2];
		qual = new byte[totsize];
		row[0] =	(byte)2;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=oi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		row[i+totsize+1]=si[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		qual[i]=pi[i];
		}
		row[rowlength] =(byte)1;
		row[rowlength+1] =(byte)1;
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);
    	try {
    		for (int i = 0; i < statrow.length; i++) {
				statrow[i]=row[i];
			}
			statistics.incrementColumnValue(statrow, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(1));
    		for (int i = 0; i < statrowfull.length; i++) {
    			statrowfull[i]=row[i];
			}
			statistics.incrementColumnValue(statrowfull, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(1));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			table.put(list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void delete(Triple triple) throws NotSupportedDatatypeException {
		H2RDFNode nodeS = new H2RDFNode(triple.getSubject());
		H2RDFNode nodeP = new H2RDFNode(triple.getPredicate());
		H2RDFNode nodeO = new H2RDFNode(triple.getObject());
    	byte[] si = nodeS.getHashValue();
    	byte[] pi = nodeP.getHashValue();
    	byte[] oi = nodeO.getHashValue();
    	List<Delete> list = new  LinkedList<Delete>();
		
    	//reverse hash values
    	byte[] s = nodeS.getStringValue();
    	byte[] p = nodeP.getStringValue();
    	byte[] o = nodeO.getStringValue();
    	byte[] row = new byte[totsize+1];
    	byte[] qual = new byte[s.length];
    	Delete delete;
		//dhmiourgia spo byte[0]=4 emit row=si,pi col=oi
		row = new byte[rowlength+2];
		byte[] statrow= new byte[totsize+1];
		byte[] statrowfull= new byte[rowlength-2];
		qual = new byte[totsize];
		row[0] =	(byte)4;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=si[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		row[i+totsize+1]=pi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		qual[i]=oi[i];
		}
    	scanAndDelete(row, qual, list);
    	try {
    		for (int i = 0; i < statrow.length; i++) {
				statrow[i]=row[i];
			}
			statistics.incrementColumnValue(statrow, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(-1));
    		for (int i = 0; i < statrowfull.length; i++) {
    			statrowfull[i]=row[i];
			}
			statistics.incrementColumnValue(statrowfull, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(-1));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		//dhmiourgia pos byte[0]=3 emit row=pi,oi col=si
		row = new byte[rowlength+2];
		qual = new byte[totsize];
		row[0] =	(byte)3;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=pi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		row[i+totsize+1]=oi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		qual[i]=si[i];
		}
    	scanAndDelete(row, qual, list);
    	try {
    		for (int i = 0; i < statrow.length; i++) {
				statrow[i]=row[i];
			}
			statistics.incrementColumnValue(statrow, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(-1));
    		for (int i = 0; i < statrowfull.length; i++) {
    			statrowfull[i]=row[i];
			}
			statistics.incrementColumnValue(statrowfull, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(-1));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
		//dhmiourgia osp byte[0]=2 emit row=oi,si col=pi
		row = new byte[rowlength+2];
		qual = new byte[totsize];
		row[0] =	(byte)2;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=oi[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		row[i+totsize+1]=si[i];
		}
    	for (int i = 0; i < totsize; i++) {
    		qual[i]=pi[i];
		}
    	scanAndDelete(row, qual, list);
    	try {
    		for (int i = 0; i < statrow.length; i++) {
				statrow[i]=row[i];
			}
			statistics.incrementColumnValue(statrow, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(-1));
    		for (int i = 0; i < statrowfull.length; i++) {
    			statrowfull[i]=row[i];
			}
			statistics.incrementColumnValue(statrowfull, Bytes.toBytes("size"), Bytes.toBytes(""), new Long(-1));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		/*try {
			table.delete(list);
			table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	private void scanAndDelete(byte[] row, byte[] qual, List<Delete> list) {
		byte[] startr = new byte[rowlength+2];
		byte[] stopr = new byte[rowlength+2];
		
		for (int i = 0; i < rowlength; i++) {
			startr[i]=row[i];
			stopr[i]=row[i];
		}
		startr[startr.length-2] =(byte)0;
		startr[startr.length-1] =(byte)0;
		stopr[stopr.length-2] =(byte)255;
		stopr[stopr.length-1] =(byte)255;
		
		Scan scan = new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		scan.addColumn(Bytes.toBytes("A"), qual);
		scan.setMaxVersions(1000);
		ResultScanner resultScanner=null;
		try {
			int count=1;
			while(count>0){
				count=0;
				resultScanner = table.getScanner(scan);
				Result re;
				while((re = resultScanner.next())!=null){
					if(re.size()!=0){
						Iterator<KeyValue> it = re.list().iterator();
						while(it.hasNext()){
							KeyValue kv = it.next();
							if(kv.getQualifier().length==totsize){
								byte[] r = kv.getRow();
								kv.getTimestamp();
								Delete delete =new Delete(r);
								delete.setTimestamp(kv.getTimestamp());
								//System.out.println(Bytes.toStringBinary(r)+" qual"+Bytes.toStringBinary(qual)+" qual"+Bytes.toStringBinary(kv.getQualifier()));
								delete.deleteColumn(Bytes.toBytes("A"), qual);
								count++;
								try {
									table.delete(delete);
									//table.flushCommits();
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								//list.add(delete);
							}
						}
						
					}
				}
			}
			return;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}
		
		
	}
}
