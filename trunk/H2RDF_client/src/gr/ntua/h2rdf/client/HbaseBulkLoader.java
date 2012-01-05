package gr.ntua.h2rdf.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;

import com.hp.hpl.jena.graph.Triple;

public class HbaseBulkLoader implements Loader {
	
	private HTable table;
	private H2RDFConf conf;
	private int triples, chunkSize;
	private List<Put> list;
	private HashMap<ByteArray, Long> incrList;
	
	
	public HbaseBulkLoader(H2RDFConf conf) {
		this.conf=conf;
		try {
			Configuration hbconf= HBaseConfiguration.create();
			HBaseAdmin hadmin = new HBaseAdmin(hbconf);
			System.out.println(conf.getAddress());
			System.out.println(conf.getName());
			if(!hadmin.tableExists(conf.getName()+"")){
				System.out.println("creating "+conf.getName());
				HTableDescriptor desc = new HTableDescriptor(conf.getName());
				HColumnDescriptor family= new HColumnDescriptor("A");
				desc.addFamily(family); 
				hadmin.createTable(desc);
			}
			table = new HTable(conf.getName());
			triples=0;
	    	list = new  LinkedList<Put>();
	    	incrList = new  HashMap<ByteArray, Long>();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void add(Triple triple) {
		Random g2 = new Random();
	
		String subject =triple.getSubject().toString();
		String predicate =triple.getPredicate().toString();
		String object =triple.getObject().toString();
    	byte[] si = getHash(subject);
    	byte[] pi = getHash(predicate);
    	byte[] oi = getHash(object);
		
    	//reverse hash values
    	byte[] s=Bytes.toBytes(subject);
    	byte[] p=Bytes.toBytes(predicate);
    	byte[] o=Bytes.toBytes(object);
    	byte[] row = new byte[8+1];
    	byte[] qual = new byte[s.length];
    	row[0] =(byte)1;
    	for (int i = 0; i < 8; i++) {
    		row[i+1]=si[i];
		}
    	for (int i = 0; i < s.length; i++) {
    		qual[i]=s[i];
    	}
    	Put put =new Put(row);
		put.add(Bytes.toBytes("A"),Bytes.toBytes("i"), qual);
		
		list.add(put);
		
		row = new byte[8+1];
    	qual = new byte[p.length];
    	row[0] =(byte)1;
    	for (int i = 0; i < 8; i++) {
    		row[i+1]=pi[i];
		}
    	for (int i = 0; i < p.length; i++) {
    		qual[i]=p[i];
    	}
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), Bytes.toBytes("i"), qual);
		
		list.add(put);
		
		row = new byte[8+1];
    	qual = new byte[o.length];
    	row[0] =(byte)1;
    	for (int i = 0; i < 8; i++) {
    		row[i+1]=oi[i];
		}
    	for (int i = 0; i < o.length; i++) {
    		qual[i]=o[i];
    	}
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), Bytes.toBytes("i"), qual);
		
		list.add(put);
		
		//dhmiourgia spo byte[0]=4 emit row=si,pi col=oi
		row = new byte[1+8+8+2];
		qual = new byte[8];
		row[0] =	(byte)4;
    	for (int i = 0; i < 8; i++) {
    		row[i+1]=si[i];
		}
    	for (int i = 0; i < 8; i++) {
    		row[i+8+1]=pi[i];
		}
    	for (int i = 0; i < 8; i++) {
    		qual[i]=oi[i];
		}
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);
		
		row[17] =(byte)255;
		row[18] =(byte)255;

		ByteArray b;
		Long incr;
		
		b = new ByteArray(row);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		for (int i = 9; i < row.length; i++) {
			row[i]=(byte)255;
		}
		b = new ByteArray(row);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		
		
		
		//dhmiourgia pos byte[0]=3 emit row=pi,oi col=si
		row = new byte[1+8+8+2];
		qual = new byte[8];
		row[0] =	(byte)3;
    	for (int i = 0; i < 8; i++) {
    		row[i+1]=pi[i];
		}
    	for (int i = 0; i < 8; i++) {
    		row[i+8+1]=oi[i];
		}
    	for (int i = 0; i < 8; i++) {
    		qual[i]=si[i];
		}
		row[17] =(byte) (short) g2.nextInt(255);
		row[18] =(byte) (short) g2.nextInt(254);
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);

		row[17] =(byte)255;
		row[18] =(byte)255;

		b = new ByteArray(row);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		for (int i = 9; i < row.length; i++) {
			row[i]=(byte)255;
		}
		b = new ByteArray(row);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		
		//dhmiourgia osp byte[0]=2 emit row=oi,si col=pi
		row = new byte[1+8+8+2];
		qual = new byte[8];
		row[0] =	(byte)2;
    	for (int i = 0; i < 8; i++) {
    		row[i+1]=oi[i];
		}
    	for (int i = 0; i < 8; i++) {
    		row[i+8+1]=si[i];
		}
    	for (int i = 0; i < 8; i++) {
    		qual[i]=pi[i];
		}
    	put =new Put(row);
		put.add(Bytes.toBytes("A"), qual, null);
		
		list.add(put);

		row[17] =(byte)255;
		row[18] =(byte)255;
		
		b = new ByteArray(row);
		incr = incrList.get(b);
		if(incr==null){
			incrList.put(b, new Long(1));
		}
		else{
			incrList.put(b, incr+1);
		}
		for (int i = 9; i < row.length; i++) {
			row[i]=(byte)255;
		}
		b = new ByteArray(row);
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
				//System.out.println("Bulk put");
				table.put(list);
				Iterator<ByteArray> it =incrList.keySet().iterator();
				while(it.hasNext()){
					ByteArray k=it.next();
					//System.out.println("Increment "+Bytes.toStringBinary(k.getArray())+" "+incrList.get(k));
					table.incrementColumnValue(k.getArray() , Bytes
							.toBytes("A"), Bytes.toBytes("s"), incrList.get(k));
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
	
	private byte[] getHash(String string) {
		
		MD5Hash md5h = MD5Hash.digest(string);
		long hashVal = Math.abs(md5h.halfDigest());
		
		byte[] b = Bytes.toBytes(hashVal);
		if (b.length<8){
			System.exit(5);
		}
		else if (b.length>8){
			System.exit(6);
		}
		
		return b;
	}
	
	@Override
	public void close() {
		
		try {
			table.put(list);
			Iterator<ByteArray> it =incrList.keySet().iterator();
			while(it.hasNext()){
				ByteArray k=it.next();
				table.incrementColumnValue(k.getArray(), Bytes
						.toBytes("A"), Bytes.toBytes("s"), incrList.get(k));
			}
			table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize=chunkSize;
		
	}

}
