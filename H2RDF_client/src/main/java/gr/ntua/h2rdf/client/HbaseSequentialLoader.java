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
package gr.ntua.h2rdf.client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;

import gr.ntua.h2rdf.bytes.ByteValues;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.graph.Triple;

public class HbaseSequentialLoader implements Loader {
	private HTable table, statistics;
	private H2RDFConf conf;
	private final int totsize=ByteValues.totalBytes, rowlength=1+2*totsize;

	public HbaseSequentialLoader(H2RDFConf conf) {
		this.conf=conf;
		try {
			String t =conf.getTable();
			Configuration hbconf= HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(hbconf);
			System.out.println(conf.getAddress());
			System.out.println(t);
			if(!hadmin.tableExists(t+"")){
				System.out.println("creating "+t);
				HTableDescriptor desc = new HTableDescriptor(t);
				HColumnDescriptor family= new HColumnDescriptor("A");
				desc.addFamily(family); 
				hadmin.createTable(desc);
			}
			if(!hadmin.tableExists(t+"_stats")){
				System.out.println("creating "+t+"_stats");
				HTableDescriptor desc = new HTableDescriptor(t+"_stats");
				HColumnDescriptor family= new HColumnDescriptor("size");
				desc.addFamily(family); 
				hadmin.createTable(desc);
			}
			table = new HTable(t);
			statistics = new HTable(t+"_stats");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void add(Triple triple) throws NotSupportedDatatypeException {

		String subject =triple.getSubject().toString();
		String predicate =triple.getPredicate().toString();
		String object =triple.getObject().toString();
    	byte[] si = ByteValues.getFullValue(subject);
    	byte[] pi = ByteValues.getFullValue(predicate);
    	byte[] oi = ByteValues.getFullValue(object);
    	List<Put> list = new  LinkedList<Put>();
		
    	//reverse hash values
    	byte[] s=Bytes.toBytes(subject);
    	byte[] p=Bytes.toBytes(predicate);
    	byte[] o=Bytes.toBytes(object);
    	byte[] row = new byte[totsize+1];
    	byte[] qual = new byte[s.length];
    	row[0] =(byte)1;
    	for (int i = 0; i < totsize; i++) {
    		row[i+1]=si[i];
		}
    	for (int i = 0; i < s.length; i++) {
    		qual[i]=s[i];
    	}
    	Put put =new Put(row);
		put.add(Bytes.toBytes("A"),Bytes.toBytes("i"), qual);
		
		list.add(put);
		
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
}
