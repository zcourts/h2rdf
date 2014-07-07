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
import java.util.List;
import java.util.NavigableSet;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ClientScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

public class ResultRecordReader implements TableRecordReaderInt{
	private byte[] vars;
	private Long start;
	private Iterator<Result> it;
	private List<Bindings> key, newKey;
	private BytesWritable value;
	private int keys, seeks;
	private short keyPos;
	private boolean hasMore;
	private long[] kv;
	private Iterator<KeyValue> res;
	private ResultScanner scanner;
	public Long jVar, newJVar;
	private byte[] startRow;
	private HTable table;
	private Scan scan;
	private double[] stats;
	private double jumpOffset;
	private HashMap<Integer, Integer> varRelabeling;//key:file varId, value newqueryVarId
	private List<Integer> ordering;
	public HashMap<Integer, Long> selectiveBindings;
	private int good=0;

	public ResultRecordReader(Scan scan, HTable table) throws IOException {
		this.scan = scan;
		parseRelabeling(scan);
		parseOrdering(scan);
		parseSelectiveBindings(scan);
		stats = new double[2];
		stats[0] = Bytes.toDouble(scan.getAttribute("stat0"));
		stats[1] = Bytes.toDouble(scan.getAttribute("stat1"));
		jumpOffset = TableRecordReader2.seekOverhead/stats[1];
		startRow = scan.getStartRow();
		this.table=table;
		keyPos = Bytes.toShort(scan.getAttribute("h2rdf.keyPos"));
		scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
		scan.addFamily(Bytes.toBytes("I"));
		  
		scanner = table.getScanner(scan);
		it =scanner.iterator();
		
		hasMore = it.hasNext();
		if(hasMore){
			res = it.next().list().iterator();
		}
		jVar = null;
		newJVar = null;
		value = new BytesWritable();
		kv = null;
		keys=0;
		seeks=0;
		start  = System.currentTimeMillis();
		
	}


	private void parseSelectiveBindings(Scan scan2) {
		int size = Bytes.toInt(scan.getAttribute("h2rdf.selectiveBindingsSize"));
		selectiveBindings = new HashMap<Integer, Long>(size);
		for (int i = 0; i < size; i++) {
			String s = Bytes.toString(scan.getAttribute("h2rdf.selectiveBindings"+i));
			String[] s1 = s.split("_");
			selectiveBindings.put(Integer.parseInt(s1[0]), Long.parseLong(s1[1]));
		}
		
	}


	public ResultRecordReader(Scan scan, HTable table, long startKey) throws IOException, InterruptedException {
		this.scan = scan;
		parseRelabeling(scan);
		parseOrdering(scan);
		parseSelectiveBindings(scan);
		stats = new double[2];
		stats[0] = Bytes.toDouble(scan.getAttribute("stat0"));
		stats[1] = Bytes.toDouble(scan.getAttribute("stat1"));
		jumpOffset = TableRecordReader2.seekOverhead/stats[1];
		startRow = scan.getStartRow();
		this.table=table;
		keyPos = Bytes.toShort(scan.getAttribute("h2rdf.keyPos"));
		scan.setFamilyMap(new HashMap<byte[], NavigableSet<byte[]>>());
		scan.addFamily(Bytes.toBytes("I"));
		  
		scanner = table.getScanner(scan);
		it =scanner.iterator();
		
		hasMore = it.hasNext();
		if(hasMore){
			res = it.next().list().iterator();
		}
		jVar = null;
		newJVar = null;
		value = new BytesWritable();
		kv = null;
		keys=0;
		seeks=0;
		start  = System.currentTimeMillis();
		hasMore = goTo(startKey);
	}

	@Override
	public List<Bindings> getCurrentKey() {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() {
		return value;
	}

	public List<Bindings> nextKValue() throws IOException {
		if(!hasMore)
			return null;

		if(res.hasNext()){
			KeyValue kv1 = res.next();
			byte[] tempk = kv1.getRow();
			byte[] tempv = kv1.getValue();
			startRow=tempk;
			Bindings keyBinding = new Bindings(varRelabeling,selectiveBindings);
			InputStream is = new ByteArrayInputStream(tempk);
			for (int i = 0; i < ordering.size(); i++) {
				long t =SortedBytesVLongWritable.readLong(is);
				if(i==keyPos){
					newJVar=t;
					//System.out.println(t);
				}
				else if(i>keyPos){
					keyBinding.addBinding((byte)(int)varRelabeling.get(ordering.get(i)), t);
					//System.out.println(bindings.map);
				}
			}
			List<Bindings> bindingList = new ArrayList<Bindings>();
			is = new ByteArrayInputStream(tempv);
			while(is.available()>0){
				Bindings bindings = new Bindings(varRelabeling,selectiveBindings);
				bindings.readFields(is,varRelabeling,selectiveBindings);
				if(!bindings.valid)
					continue;
				bindings.addAll(keyBinding);
				bindingList.add(bindings);
			}
			/*System.out.println(Bytes.toStringBinary(tempv));
			for(Bindings b :bindingList){
				System.out.println(b.map);
			}*/
			
			return bindingList;
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
		
		if(newJVar != null){
			jVar = newJVar;
			key=newKey;
			newJVar=null;
		}
		
		int count = 0;
		while((newKey = nextKValue()) != null){
			if(jVar == null){
				jVar = newJVar;
				key = newKey;
			}
			else if(jVar == newJVar){//same key
				key.addAll(newKey);
			}
			else{//new key
				return true;
			}
			
			count++;
			if(!hasMore)
				return true;
			
		}
		
		return !key.isEmpty() && count>0;
		
	}

	@Override
	public boolean goTo(long k) throws IOException, InterruptedException {
		//System.out.println("Go to: "+k);
		/*InputStream is = new ByteArrayInputStream(startRow);
		long[] r = new long[keyPos+1];
		for (int i = 0; i < keyPos; i++) {
			r[i]=SortedBytesVLongWritable.readLong(is);
		}
		r[keyPos] = k;
		List<Byte> newRowList=new ArrayList<Byte>();
		for (int i = 0; i < r.length; i++) {
			SortedBytesVLongWritable s = new SortedBytesVLongWritable(r[i]);
			byte[] lb= s.getBytesWithPrefix();
			for (int j = 0; j < lb.length; j++) {
				newRowList.add(lb[j]);
			}
		}
		byte[] newRow = new byte[newRowList.size()];
		int i=0;
		for(Byte b : newRowList){
			newRow[i]=b;
			i++;
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
			newJVar = null;
			value = new BytesWritable();
			kv = null;
			key = new ArrayList<Bindings>();
			boolean ret =nextKeyValue();
			//System.out.println("Current: "+jVar);
			if(k==jVar){
				good++;
				System.out.println("Good: "+good);
				//System.out.println("Good jump");
			}
			return ret;
		}*/

		while(nextKeyValue()){
			if(this.jVar >= k){
				if(k==jVar){
					good++;
					//System.out.println("Good: "+good);
					//System.out.println("Good jump");
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public void close() throws IOException {
		double sec = ((double)System.currentTimeMillis()-start)/(double)1000;
		double thr = ((double)keys)/sec;
		//System.out.println("Good!!: "+good);
		System.out.println("Troughput: "+thr);
		System.out.println("Records: "+keys+" seeks: "+seeks+" time: "+sec );
		scanner.close();
	}

	@Override
	public Long getJvar() {
		return jVar;
	}

	private void parseRelabeling(Scan scan) {
		byte[] rel = scan.getAttribute("h2rdf.varRelabeling");
		varRelabeling = new HashMap<Integer, Integer>();
		for (int i = 0; i < rel.length; i+=2) {
			varRelabeling.put((int)rel[i], (int)rel[i+1]);
		}
		//System.out.println("Varelabeling: "+varRelabeling);
	}

	private void parseOrdering(Scan scan) {
		byte[] ord = scan.getAttribute("h2rdf.ordering");
		ordering = new ArrayList<Integer>();
		for (int i = 0; i < ord.length; i++) {
			ordering.add((int)ord[i]);
		}
		//System.out.println("Ordering: "+ordering);
		
	}
}
