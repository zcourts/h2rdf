/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.inputFormat2;

import gr.ntua.h2rdf.indexScans.Bindings;
import gr.ntua.h2rdf.loadTriples.ByteTriple;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TableRecordReader2 extends RecordReader<Bindings, BytesWritable>{

	  private int numBound;
	  private static final int seekOverhead =250000;
	  private Bindings current, next;
	  private TableRecordReaderImpl recordReaderImpl = new TableRecordReaderImpl();
	private byte[] vars;
	
	
	
	private long nextJoinVar;
	private Long start;
	private Iterator<Result> it;
	private Bindings key;
	private BytesWritable value;
	private int keys, seeks;
	private int keyPos;
	private int valuePos;
	private boolean hasMore;
	private long[] kv;
	private Iterator<KeyValue> res;
	private ResultScanner scanner;
	public Long jVar;
	private byte[] startRow;
	private HTable table;
	private Scan scan;
	private double[] stats;
	private double jumpOffset;


	public TableRecordReader2(Scan scan , byte[] t) throws IOException {
		this.scan = scan;
		stats = new double[2];
		stats[0] = Bytes.toDouble(scan.getAttribute("stat0"));
		stats[1] = Bytes.toDouble(scan.getAttribute("stat1"));
		jumpOffset = seekOverhead/stats[1];
		startRow = scan.getStartRow();
		Configuration hconf = HBaseConfiguration.create();
		table = new HTable(hconf, t);
		vars = scan.getFamilies()[0];
		keyPos = 3-(vars.length);
		scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
		scan.addFamily(Bytes.toBytes("I"));
		  
		scanner = table.getScanner(scan);
		it =scanner.iterator();
		
		hasMore = it.hasNext();
		if(hasMore){
			res = it.next().list().iterator();
		}
		jVar = null;
		value = new BytesWritable();
		kv = null;
		keys=0;
		seeks=0;
		start  = System.currentTimeMillis();
	}


	public TableRecordReader2(Scan scan,  byte[] t,
			long startKey) throws IOException {
		this.scan = scan;
		stats = new double[2];
		stats[0] = Bytes.toDouble(scan.getAttribute("stat0"));
		stats[1] = Bytes.toDouble(scan.getAttribute("stat1"));
		jumpOffset = seekOverhead/stats[1];
		startRow = scan.getStartRow();
		Configuration hconf = HBaseConfiguration.create();
		table = new HTable(hconf, t);
		vars = scan.getFamilies()[0];
		keyPos = 3-(vars.length);
		scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
		scan.addFamily(Bytes.toBytes("I"));
		  
		InputStream is = new ByteArrayInputStream(startRow,1,startRow.length-1);
		long[] r = new long[3];
		for (int i = 0; i < keyPos; i++) {
			r[i]=SortedBytesVLongWritable.readLong(is);
		}
		r[keyPos] = startKey;
		byte[] newRow=null;
		switch (keyPos) {
		case 0:
			newRow = ByteTriple.createByte(r[0], startRow[0]);
			break;
		case 1:
			newRow = ByteTriple.createByte(r[0], r[1],startRow[0]);
			break;
		case 2:
			newRow = ByteTriple.createByte(r[0], r[1], r[2], startRow[0]);
			break;

		default:
			break;
		}
		
		
		scan.setStartRow(newRow);
		scanner = table.getScanner(scan);
		it =scanner.iterator();
		
		hasMore = it.hasNext();
		if(hasMore){
			res = it.next().list().iterator();
		}
		jVar = null;
		value = new BytesWritable();
		kv = null;
		keys=0;
		seeks=0;
		start  = System.currentTimeMillis();
	}


	public TableRecordReader2(Scan scan, HTable table, long startKey) throws IOException {
		this.scan = scan;
		stats = new double[2];
		stats[0] = Bytes.toDouble(scan.getAttribute("stat0"));
		stats[1] = Bytes.toDouble(scan.getAttribute("stat1"));
		jumpOffset = seekOverhead/stats[1];
		startRow = scan.getStartRow();
		this.table=table;
		vars = scan.getFamilies()[0];
		keyPos = 3-(vars.length);
		scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
		scan.addFamily(Bytes.toBytes("I"));
		  
		InputStream is = new ByteArrayInputStream(startRow,1,startRow.length-1);
		long[] r = new long[3];
		for (int i = 0; i < keyPos; i++) {
			r[i]=SortedBytesVLongWritable.readLong(is);
		}
		r[keyPos] = startKey;
		byte[] newRow=null;
		switch (keyPos) {
		case 0:
			newRow = ByteTriple.createByte(r[0], startRow[0]);
			break;
		case 1:
			newRow = ByteTriple.createByte(r[0], r[1],startRow[0]);
			break;
		case 2:
			newRow = ByteTriple.createByte(r[0], r[1], r[2], startRow[0]);
			break;

		default:
			break;
		}
		
		
		scan.setStartRow(newRow);
		scanner = table.getScanner(scan);
		it =scanner.iterator();
		
		hasMore = it.hasNext();
		if(hasMore){
			res = it.next().list().iterator();
		}
		jVar = null;
		value = new BytesWritable();
		kv = null;
		keys=0;
		seeks=0;
		start  = System.currentTimeMillis();
	}


	public TableRecordReader2(Scan scan, HTable table) throws IOException {
		this.scan = scan;
		stats = new double[2];
		stats[0] = Bytes.toDouble(scan.getAttribute("stat0"));
		stats[1] = Bytes.toDouble(scan.getAttribute("stat1"));
		jumpOffset = seekOverhead/stats[1];
		startRow = scan.getStartRow();
		this.table=table;
		vars = scan.getFamilies()[0];
		keyPos = 3-(vars.length);
		scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
		scan.addFamily(Bytes.toBytes("I"));
		  
		scanner = table.getScanner(scan);
		it =scanner.iterator();
		
		hasMore = it.hasNext();
		if(hasMore){
			res = it.next().list().iterator();
		}
		jVar = null;
		value = new BytesWritable();
		kv = null;
		keys=0;
		seeks=0;
		start  = System.currentTimeMillis();
	}


	@Override
	public Bindings getCurrentKey() {
		return key;
	}


	@Override
	public BytesWritable getCurrentValue() {
		return value;
	}


	public long[] nextKValue() throws IOException {
		if(!hasMore)
			return null;

		if(res.hasNext()){
			KeyValue kv1 = res.next();
			byte[] temp = kv1.getRow();
			return ByteTriple.parseRow(temp);
		}
		
		if(it.hasNext()){
			/*if(res.hasNext()){
				KeyValue kv = res.next();
				byte[] temp = kv.getRow();
				return ByteTriple.parseRow(temp);
			}
			else{*/
				hasMore = it.hasNext();
				if(!hasMore)
					return null;
				if(hasMore){
					res = it.next().list().iterator();
				}
				return nextKValue();
			//}
			
			
		}
		else{
			hasMore = it.hasNext();
			if(!hasMore)
				return null;
			if(hasMore){
				res = it.next().list().iterator();
			}
			
			return nextKValue();
		}
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		keys++;
		if(!hasMore)
			return hasMore;
		
		if(jVar != null){
			key = new Bindings();
			for (int i = 0; i < vars.length; i++) {
				key.addBinding(vars[i], kv[keyPos+i]);
			}
			jVar = new Long(kv[keyPos]);
		}
		
		int count = 0;
		while((kv = nextKValue()) != null){
			if(jVar == null){
				key = new Bindings();
				for (int i = 0; i < vars.length; i++) {
					key.addBinding(vars[i], kv[keyPos+i]);
				}
				jVar = new Long(kv[keyPos]);
			}
			else if(jVar == kv[keyPos]){//same key
				for (int i = 1; i < vars.length; i++) {
					key.addBinding(vars[i], kv[keyPos+i]);
				}
			}
			else{//new key
				return true;
			}
			
			count++;
			if(!hasMore)
				return true;
			
		}
		
		return !key.map.isEmpty() && count>0;
		
	}


	public boolean goTo(long k) throws IOException, InterruptedException {
		//System.out.print("from:"+jVar);
		//System.out.println(" go to:"+k);
		if(Math.abs(k-jVar)>=jumpOffset){
			//System.out.println("jump");
			InputStream is = new ByteArrayInputStream(startRow,1,startRow.length-1);
			long[] r = new long[3];
			for (int i = 0; i < keyPos; i++) {
				r[i]=SortedBytesVLongWritable.readLong(is);
			}
			r[keyPos] = k;
			byte[] newRow=null;
			switch (keyPos) {
			case 0:
				newRow = ByteTriple.createByte(r[0], startRow[0]);
				break;
			case 1:
				newRow = ByteTriple.createByte(r[0], r[1],startRow[0]);
				break;
			case 2:
				newRow = ByteTriple.createByte(r[0], r[1], r[2], startRow[0]);
				break;

			default:
				break;
			}
			seeks++;
			scan.setStartRow(newRow);
			scanner = table.getScanner(scan);
			it =scanner.iterator();
			
			hasMore = it.hasNext();
			if(hasMore){
				res = it.next().list().iterator();
			}
			jVar = null;
			value = new BytesWritable();
			kv = null;
			return nextKeyValue();
		}
		else{
			while(nextKeyValue()){
				if(this.jVar >= k)
					return true;
			}
			return false;
		}
		/*InputStream is = new ByteArrayInputStream(startRow,1,startRow.length-1);
		long[] r = new long[3];
		for (int i = 0; i < keyPos; i++) {
			r[i]=SortedBytesVLongWritable.readLong(is);
		}
		r[keyPos] = k;
		byte[] newRow=null;
		switch (keyPos) {
		case 0:
			newRow = ByteTriple.createByte(r[0], startRow[0]);
			break;
		case 1:
			newRow = ByteTriple.createByte(r[0], r[1],startRow[0]);
			break;
		case 2:
			newRow = ByteTriple.createByte(r[0], r[1], r[2], startRow[0]);
			break;

		default:
			break;
		}
		seeks++;
		if(kv!=null && kv[keyPos]>= k){
			return nextKeyValue();
		}
		long jump = (long)(Math.abs(k-jVar)*stats[1]);
		Result rt = ((ClientScanner)scanner).seekTo(newRow,jump) ;
		if(rt==null)
			return false;
		else{
			it =scanner.iterator();
			
			hasMore = true;
			res = rt.list().iterator();
			jVar = null;
			value = new BytesWritable();
			kv = null;
			key = new Bindings();
			return nextKeyValue();
		}*/
	}


	@Override
	public void close() throws IOException {

		double sec = ((double)System.currentTimeMillis()-start)/(double)1000;
		double thr = ((double)keys)/sec;
		System.out.println("Troughput: "+thr);
		System.out.println("Records: "+keys+" seeks: "+seeks+" time: "+sec );
		scanner.close();
	}


	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}


	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}


}
