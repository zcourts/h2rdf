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

import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.indexScans.Bindings;
import gr.ntua.h2rdf.indexScans.JoinExecutor;
import gr.ntua.h2rdf.indexScans.ResultBGP;
import gr.ntua.h2rdf.inputFormat2.ResultRecordReader;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.MetaReader.Visitor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.function.library.e;

public class CachedResult implements DPJoinPlan{

	public String path, table;
	private Double cost;
	public Map<Integer, double[]> stats;
	public List<ResultBGP> results;
	public TreeMap<Integer, Integer> cachedCanonicalVariableMapping;//key:canonicalId value:joinId
	public List<Integer> ordering;
	public boolean ordered;
	private FileSystem fs;
	public TreeMap<Integer, Integer> currentCanonicalVariableMapping;//key:canonicalId value:joinId
	public HashMap<Integer, Long> selectiveBindings;
	public HashMap<Integer, Integer> varRelabeling;//key:file varId, value newqueryVarId

	public CachedResult(List<ResultBGP> results, Map<Integer, double[]> stats, OptimizeOpVisitorDPCaching visitor) throws IOException {
		this.results=results;
		cost=0.0;
		this.stats=stats;
		ordered=false;
		Configuration conf = new Configuration();
		fs = FileSystem.get(conf);
		table=Bytes.toString(visitor.table.getTableName());
		ordering = new ArrayList<Integer>();
		/*for(Var var : results.get(0).joinVars){
			Integer tempId =visitor.varRevIds.get(var);
			
		}*/
	}

	public void clearTempData(){
		currentCanonicalVariableMapping=null;
		selectiveBindings=null;
	}
	
	@Override
	public String print() {
		String ret="";
		if(ordered){
			ret+="OrderedCachedResult: "+ordering+" table: "+table;
		}
		else{
			ret+="CachedResult: "+results.get(0).path;
		}
		return ret;
	}

	@Override
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws IOException, InterruptedException {
		System.out.println("Executing: "+print());
		if(ordered){
			Configuration conf = new Configuration();
			fs=FileSystem.get(conf);
			Path out =  new Path("output/join_"+cachingExecutor.id+"_"+cachingExecutor.tid);
			cachingExecutor.tid++;
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, out, Bindings.class, BytesWritable.class);
			
			HTable t = CachingExecutor.getTable(table);
			Scan scan = getScan();
			scan.setCacheBlocks(true); 
			scan.setAttribute("stat0", Bytes.toBytes(new Double(100.0)));
			scan.setAttribute("stat1", Bytes.toBytes(new Double(100.0)));
			List<Long> l = new ArrayList<Long>();
			for(Integer i : ordering){
				int id=0;
				for(Entry<Integer, Integer> c : cachedCanonicalVariableMapping.entrySet()){
					if(c.getValue().equals(i)){
						id = currentCanonicalVariableMapping.get(c.getKey());
						break;
					}
				}
				Long l1 = selectiveBindings.get(id);
				if(l1!=null){
					l.add(l1);
				}
			}

			List<Byte> newRowList=new ArrayList<Byte>();
			for (Long l1 : l) {
				SortedBytesVLongWritable s = new SortedBytesVLongWritable(l1);
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
			scan.setStartRow(newRow);
			scan.setStopRow(newRow);
			
			ResultRecordReader reader = new ResultRecordReader(scan, t);
			int count=0;
			while(reader.nextKeyValue()){
				//System.out.println(reader.jVar);
				for(Bindings b :reader.getCurrentKey()){
					writer.append(b, new BytesWritable(new byte[0]));
					count++;
					//System.out.println(b.map);
				}
			}
			System.out.println("Results: "+count);

			writer.close();
			
		}
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		return cost.compareTo(o.getCost());
	}
	@Override
	public Double getCost() {
		return cost;
	}

	@Override
	public double[] getStatistics(Integer joinVar) throws IOException {
		for(Entry<Integer, Integer> c : currentCanonicalVariableMapping.entrySet()){
			if(c.getValue().equals(joinVar)){
				double[] st	=stats.get(cachedCanonicalVariableMapping.get(c.getKey()));
	    		if(st==null){
	    			st = new double[2];
	    			st[0]=50.0;
	    			st[1]=1.0;
	    		}
				return st;
			}
		}
		double[] st	=stats.entrySet().iterator().next().getValue();
		if(st==null){
			st = new double[2];
			st[0]=50.0;
			st[1]=1.0;
		}
		return st;
	}
	@Override
	public List<ResultBGP> getResults() throws IOException {
		return results;
	}

	@Override
	public void computeCost() throws IOException {
		//selection cost
		if(selectiveBindings==null){
			cost=0.0;
		}
		else{
			if(!ordered){
				double readKeysScan=1;
				for(Entry<Integer, double[]> e : stats.entrySet()){
					double[] stat = e.getValue();
					readKeysScan+=stat[0]*stat[1];
					break;
				}
				//cost=readKeysScan/100000;
				//System.out.println("Cost non ordered: "+cost);
				cost=8.0;
			}
			else{
				int found=0;
				for(Integer i : ordering){
					int id=0;
					for(Entry<Integer, Integer> c : cachedCanonicalVariableMapping.entrySet()){
						if(c.getValue().equals(i)){
							id = currentCanonicalVariableMapping.get(c.getKey());
							break;
						}
					}
					if(selectiveBindings.containsKey(id)){
						found++;
					}
					else{
						break;
					}
				}
				if(found>0){
					int f =found;
					double readKeysScan=Double.MAX_VALUE;
					for(Integer i : ordering){
						double[] stat = getStatistics(i);
						if(stat[1]<readKeysScan){
							readKeysScan=stat[1];
						}
						f--;
						if(f==0){
							break;
						}
					}
					cost=readKeysScan/100000;
				}
				else{
					double readKeysScan=1;
					for(Entry<Integer, double[]> e : stats.entrySet()){
						double[] stat = e.getValue();
						readKeysScan+=stat[0]*stat[1];
						break;
					}
					cost=readKeysScan/100000;
				}
			}
		}
	}
	@Override
	public List<Integer> getOrdering() {
		return ordering;
	}

	public void setOrdering(List<Integer> ordering, CachingExecutor cachingExecutor) throws Exception {
		this.ordering=ordering;
		ordered=true;
		Path out= results.get(0).path;
		loadHFiles(out, cachingExecutor);
		fs.delete(out,true);
	}
	
	private void loadHFiles(Path out, CachingExecutor cachingExecutor)throws Exception {
		String id = out.getName().substring(out.getName().indexOf("_")+1);
		table=table+"_cache_"+id;//+cachingExecutor.id+"_"+(cachingExecutor.tid-1);
	   	String TABLE_NAME = table;
	   	Configuration conf = HBaseConfiguration.create();
 	    conf.addResource("hbase-default.xml");
 	    conf.addResource("hbase-site.xml");
 	    HBaseAdmin hadmin = new HBaseAdmin(conf);
 		Path hfofDir= new Path(out+"/I");
 	    //if (!fs.exists(hfofDir)) {
 	    //  throw new FileNotFoundException("HFileOutputFormat dir " +
 	    //      hfofDir + " not found");
 	    //}
 	   	// FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
 	    //if (familyDirStatuses == null) {
 	    //  throw new FileNotFoundException("No families found in " + hfofDir);
 	    //}
 	    int length =0;
 	    byte[][] splits = new byte[18000][];
		Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(hfofDir));
		for (Path hfile : hfiles) {
			if (hfile.getName().startsWith("_")) continue;
		
		  		HFile.Reader hfr = HFile.createReader(fs, hfile, new CacheConfig(conf));
		  		//HFile.Reader hfr = 	new HFile.Reader(fs, hfile, null, false);
		  		final byte[] first;
		  		try {
		  			hfr.loadFileInfo();
		  			first = hfr.getFirstRowKey();
		  		}  finally {
		  			hfr.close();
		  		}
				//System.out.println("out/I/"+hfile.getName()+" \t "+Bytes.toStringBinary(first));
		  		splits[length]=first.clone();
		  		length++;
		}
		byte[][] splits1 = new byte[length][];

 	    for (int i = 0; i < splits1.length; i++) {
 	    		splits1[i]=splits[i];
 		}
 	    Arrays.sort(splits1, Bytes.BYTES_COMPARATOR);
 	  
 		
 	    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
 	    
 		HColumnDescriptor family= new HColumnDescriptor("I");
		family.setCompressionType(Algorithm.SNAPPY);
 		desc.addFamily(family); 
 		//family= new HColumnDescriptor("C");
 		//desc.addFamily(family); 
 		//for (int i = 0; i < splits.length; i++) {
 		//	System.out.println(Bytes.toStringBinary(splits[i]));
 		//}
 		conf.setInt("zookeeper.session.timeout", 600000);
 		if(hadmin.tableExists(TABLE_NAME)){
 			hadmin.disableTable(TABLE_NAME);
 			hadmin.deleteTable(TABLE_NAME);
 			hadmin.createTable(desc, splits1);
 		}
 		else{
 			hadmin.createTable(desc, splits1);
 		}
 		//hadmin.createTable(desc);
 		String[] args1 = new String[2];
 		args1[0]=out.toString();
 		args1[1]=TABLE_NAME;
 		//args1[1]="new2";
 		
 		ToolRunner.run(new LoadIncrementalHFiles(HBaseConfiguration.create()), args1);
 	
	 }

	public String getCanonicalOrderingString() {
		String ret = "";
		for(Integer i : ordering){
			for(Entry<Integer, Integer> c : cachedCanonicalVariableMapping.entrySet()){
				if(c.getValue().equals(i)){
					ret+=currentCanonicalVariableMapping.get(c.getKey())+"_";
					break;
				}
			}
		}
		return ret;
	}

	public long[][] getPartitions(Integer integer) throws IOException {
		HTable t = CachingExecutor.getTable(table);
		int numRegions =0;
		List<long[]> ret = new LinkedList<long[]>();
		Pair<byte[][], byte[][]> keys = t.getStartEndKeys();
		for (int i = 0; i < keys.getFirst().length; i++) {
			byte[] sk = keys.getFirst()[i];
			byte[] ek = keys.getSecond()[i];
			long[] l = new long[2];
			if (sk == null || sk.length == 0) {
				l[0]=Long.MIN_VALUE;
			}
			else{
				ByteArrayInputStream bis = new ByteArrayInputStream(sk);
				l[0] = SortedBytesVLongWritable.readLong(bis);
			}
			if (ek == null || ek.length == 0) {
				l[1]=Long.MAX_VALUE;
			}
			else{
				ByteArrayInputStream bis = new ByteArrayInputStream(ek);
				l[1] = SortedBytesVLongWritable.readLong(bis);
			}
			numRegions++;
			ret.add(l);

			
		}
		long[][] r = new long[numRegions][2];
		Iterator<long[]> it = ret.iterator();
		int i=0;
		while(it.hasNext()){
			r[i] = it.next();
			i++;
		}
		return r;
	}

	public Scan getScan() throws IOException {
		Scan ret = new Scan();
		ret.addFamily(Bytes.toBytes("I"));
		ret.setAttribute("hbase.client.scanner.seekOverhead", Bytes.toBytes((long) 1000));
		byte[] relabel = new byte[2*varRelabeling.size()];
		int i=0;
		for(Entry<Integer,Integer> e : varRelabeling.entrySet()){
			relabel[i]=(byte)(int)e.getKey();
			i++;
			relabel[i]=(byte)(int)e.getValue();
			i++;
		}
		ret.setAttribute("h2rdf.varRelabeling", relabel);
		ret.setAttribute("h2rdf.isResult", new byte[1]);
		ret.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(table));
		
		i=0;
		for(Entry<Integer, Long>  e : selectiveBindings.entrySet()){
			String s = e.getKey() + "_"+e.getValue();
			ret.setAttribute("h2rdf.selectiveBindings"+i, Bytes.toBytes(s));
			i++;
		}
		ret.setAttribute("h2rdf.selectiveBindingsSize", Bytes.toBytes(i));
		byte[] order = new byte[ordering.size()];
		i=0;
		for(Integer o : ordering){
			order[i]=(byte)(int)o;
			i++;
		}
		ret.setAttribute("h2rdf.ordering", order);
		if(selectiveBindings==null)
			ret.setAttribute("h2rdf.keyPos", Bytes.toBytes((short)0));
		else{
			if(ordering.size()<selectiveBindings.size())
				ret.setAttribute("h2rdf.keyPos", Bytes.toBytes((short)(ordering.size()-1)));
			else
				ret.setAttribute("h2rdf.keyPos", Bytes.toBytes((short)(selectiveBindings.size()-1)));
		}
		//ret.addFamily(Bytes.toBytes("I"));
		ret.setCaching(20000); //good for mapreduce scan
		ret.setCacheBlocks(false); //good for mapreduce scan
		ret.setBatch(20000); //good for mapreduce scan
		return ret;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj.getClass()!=CachedResult.class)
			return super.equals(obj);
		CachedResult c = (CachedResult)obj;
		if(ordered!=c.ordered)
			return false;
		else{
			if(ordered){
				return getCanonicalOrderingString().equals(c.getCanonicalOrderingString());
			}
			else{
				return true;
			}
		}
	}
	
}
