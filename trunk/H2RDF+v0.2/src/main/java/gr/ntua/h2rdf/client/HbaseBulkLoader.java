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

import gr.ntua.h2rdf.bytes.ByteValues;
import gr.ntua.h2rdf.bytes.H2RDFNode;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Triple;

public class HbaseBulkLoader implements Loader {
	
	private HTable table, statistics;
	private H2RDFConf conf;
	private int triples, chunkSize;
	private List<Put> list;
	private HashMap<ByteArray, Long> incrList;
	private final int totsize=ByteValues.totalBytes, rowlength=1+2*totsize;
	private short rowid;
	private byte[] rid = null;
	
	
	public HbaseBulkLoader(H2RDFConf conf) {
		this.conf=conf;
		try {
			rowid=0;
	    	rid = Bytes.toBytes(rowid);
			String t =conf.getTable();
			Configuration hbconf= conf.getConf();//HBaseConfiguration.create();
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
			triples=0;
	    	list = new  LinkedList<Put>();
	    	incrList = new  HashMap<ByteArray, Long>();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		
    	//reverse hash values
    	byte[] s = nodeS.getStringValue();
    	byte[] p = nodeP.getStringValue();
    	byte[] o = nodeO.getStringValue();

    	Put put=null;
    	byte[] row, qual;
    	if(s!= null){
	    	row = new byte[totsize+1];
	    	qual = new byte[s.length];
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

    	if(p!= null){
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
    	if(o!= null){
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
		row[rowlength] =rid[0];
		row[rowlength+1] =rid[1];
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);
		
		byte[] statrow= new byte[totsize+1];
		byte[] statrowfull= new byte[rowlength-2];
		
		ByteArray b;
		Long incr;

		for (int i = 0; i < statrow.length; i++) {
			statrow[i]=row[i];
		}
		for (int i = 0; i < statrowfull.length; i++) {
			statrowfull[i]=row[i];
		}
		
		b = new ByteArray(statrow);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		b = new ByteArray(statrowfull);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
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
		row[rowlength] =rid[0];
		row[rowlength+1] =rid[1];
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);

		for (int i = 0; i < statrow.length; i++) {
			statrow[i]=row[i];
		}
		for (int i = 0; i < statrowfull.length; i++) {
			statrowfull[i]=row[i];
		}
		
		b = new ByteArray(statrow);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		b = new ByteArray(statrowfull);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
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
		row[rowlength] =rid[0];
		row[rowlength+1] =rid[1];
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);

		for (int i = 0; i < statrow.length; i++) {
			statrow[i]=row[i];
		}
		for (int i = 0; i < statrowfull.length; i++) {
			statrowfull[i]=row[i];
		}
		
		b = new ByteArray(statrow);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		b = new ByteArray(statrowfull);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
	
		
		
		triples++;
		if(triples>=chunkSize){
			try {
				rowid++;
		    	rid = Bytes.toBytes(rowid);
				//System.out.println("Bulk put");
				table.put(list);
				Iterator<ByteArray> it =incrList.keySet().iterator();
				while(it.hasNext()){
					ByteArray k=it.next();
					//System.out.println("Increment "+Bytes.toStringBinary(k.getArray())+" "+incrList.get(k));
					statistics.incrementColumnValue(k.getArray() , Bytes.toBytes("size"), 
							Bytes.toBytes(""), incrList.get(k));
				}
				incrList = new HashMap<ByteArray, Long>();
				triples=0;
		    	list = new  LinkedList<Put>();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void close() {
		
		try {
			table.put(list);
			Iterator<ByteArray> it =incrList.keySet().iterator();
			while(it.hasNext()){
				ByteArray k=it.next();
				statistics.incrementColumnValue(k.getArray() , Bytes.toBytes("size"), 
						Bytes.toBytes(""), incrList.get(k));
			}
			//table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize=chunkSize;
		
	}

	public void flush() {
		
		try {
			table.put(list);
			Iterator<ByteArray> it =incrList.keySet().iterator();
			while(it.hasNext()){
				ByteArray k=it.next();
				statistics.incrementColumnValue(k.getArray() , Bytes.toBytes("size"), 
						Bytes.toBytes(""), incrList.get(k));
			}
			//table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void delete(Triple triple) {
		// TODO Auto-generated method stub
		
	}

}
