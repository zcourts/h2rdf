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
package gr.ntua.h2rdf.indexScans;

import gr.ntua.h2rdf.bytes.H2RDFNode;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;
import gr.ntua.h2rdf.dpplanner.IDTranslator;
import gr.ntua.h2rdf.dpplanner.StatisticsCache;
import gr.ntua.h2rdf.loadTriples.ByteTriple;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class BGP extends ResultBGP {
	private int numVars;
	public List<ByteTriple> byteTriples;
	private HTable table, indexTable;
	private String index;
	public HashMap<String,String> varPos;
	private HashMap<String, Byte> varRevPos;
	private HashMap<Var,Double[]> computedStats;
	private PartitionFinder partitionFinder;
	private static Long type=null; 
	private static Long subClass=null;
	private OptimizeOpVisitorDPCaching visitor;

	public BGP(Triple bgp, HTable table, HTable indexTable, PartitionFinder partitionFinder, OptimizeOpVisitorDPCaching visitor)throws IOException, NotSupportedDatatypeException  {
		this.visitor=visitor;
		this.partitionFinder = partitionFinder;
		isJoined = false;
		computedStats = new HashMap<Var, Double[]>();
		byteTriples = new ArrayList<ByteTriple>();
		this.table = table;
		this.indexTable = indexTable;
		this.bgp = bgp;
		numVars=0;
		index="";
		varPos = new HashMap<String, String>();
		varRevPos = new HashMap<String, Byte>();
		ByteTriple byteTriple = new ByteTriple();
		if(type==null){
			type=IDTranslator.translate("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", indexTable);
			/*Get get = new Get(Bytes.toBytes("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			if(res.isEmpty())
				throw new IOException("node not found");
			v.setBytesWithPrefix(res.value());
			type=new Long(v.getLong());*/
		}
		if(subClass==null){
			subClass=IDTranslator.translate("<http://www.w3.org/2000/01/rdf-schema#subClassOf>", indexTable);
			/*Get get = new Get(Bytes.toBytes("<http://www.w3.org/2000/01/rdf-schema#subClassOf>"));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			//if(res.isEmpty()){
			//	throw new IOException("node not found");
			//}
			if(!res.isEmpty()){
				v.setBytesWithPrefix(res.value());
				subClass=new Long(v.getLong());
			}*/
		}
			
		
		if(!bgp.getSubject().isVariable()){
			H2RDFNode n = new H2RDFNode(bgp.getSubject());
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			v.set(IDTranslator.translate(n.getString(), indexTable));
			/*Get get = new Get(Bytes.toBytes(n.getString()));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			if(res.isEmpty())
				throw new IOException("node:"+n.getString()+" not found");
			v.setBytesWithPrefix(res.value());*/
			byteTriple.setSub(v);
			index+="s";
		}
		else{
			numVars++;
			varPos.put(bgp.getSubject().toString(), "s");
			varRevPos.put("s", (byte)((int)visitor.varRevIds.get((Var)bgp.getSubject()))); //OptimizeOpVisitorMergeJoin.varIds.get((Var)bgp.getSubject())
		} 
		
		if(!bgp.getPredicate().isVariable()){
			H2RDFNode n = new H2RDFNode(bgp.getPredicate());
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			v.set(IDTranslator.translate(n.getString(), indexTable));
			/*Get get = new Get(Bytes.toBytes(n.getString()));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			if(res.isEmpty())
				throw new IOException("node:"+n.getString()+" not found");
			v.setBytesWithPrefix(res.value());*/
			byteTriple.setPred(v);
			index+="p";
		}
		else{
			numVars++;
			varPos.put(bgp.getPredicate().toString(), "p");
			varRevPos.put("p", (byte)((int)visitor.varRevIds.get((Var)bgp.getPredicate()))); //OptimizeOpVisitorMergeJoin.varIds.get((Var)bgp.getSubject())
			
		}
		
		if(!bgp.getObject().isVariable()){
			H2RDFNode n = new H2RDFNode(bgp.getObject());
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			v.set(IDTranslator.translate(n.getString(), indexTable));
			/*Get get = new Get(Bytes.toBytes(n.getString()));
			get.addColumn(Bytes.toBytes("1"), new byte[0]);
			Result res = indexTable.get(get);
			SortedBytesVLongWritable v = new SortedBytesVLongWritable();
			if(res.isEmpty())
				throw new IOException("node:"+n.getString()+" not found");
			v.setBytesWithPrefix(res.value());*/
			byteTriple.setObj(v);
			index+="o";
		}
		else{
			numVars++;
			varPos.put(bgp.getObject().toString(), "o");
			varRevPos.put("o", (byte)((int)visitor.varRevIds.get((Var)bgp.getObject()))); //OptimizeOpVisitorMergeJoin.varIds.get((Var)bgp.getSubject())
			
		}
		byteTriples.add(byteTriple);
		
		joinVars = new HashSet<Var>();
		if(bgp.getSubject().isVariable())
			joinVars.add((Var)bgp.getSubject());
		if(bgp.getPredicate().isVariable())
			joinVars.add((Var)bgp.getPredicate());
		if(bgp.getObject().isVariable())
			joinVars.add((Var)bgp.getObject());
	}
	
	public long[][] getPartitions(Var nonJoinVar) throws IOException {
		
		if(numVars==1){
			return partitionFinder.getPartition(getScan("?"+nonJoinVar.getName(), byteTriples.get(0)).getStartRow(), 2);
		}
		if(numVars==2){
			return partitionFinder.getPartition(getScan("?"+nonJoinVar.getName(), byteTriples.get(0)).getStartRow(), 1);
		}
		return null;
	}
	
	public void printScan(String string){
		System.out.println(index+varPos.get(string));
	}
	
	
	public void processSubclass() throws IOException{
		Iterator<ByteTriple> it = byteTriples.iterator();
		List<ByteTriple> ret = new ArrayList<ByteTriple>();
		while(it.hasNext()){
			ByteTriple b = it.next();
			procSub(b, ret);
		}
		byteTriples.addAll(ret);
	}
	
	private void procSub(ByteTriple b, List<ByteTriple> ret) throws IOException {
		if(b.getP()==type && numVars==1 && b.getO()!=0){
			ByteTriple temp = new ByteTriple();
			temp.setObject(b.getO());
			temp.setPredicate(subClass);//subClassOf
			byte[] start = temp.getOPSByte();
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			Scan scan = new Scan(start,stop);
			scan.addFamily(Bytes.toBytes("I"));
			Iterator<Result> it = table.getScanner(scan).iterator();
			long sum=0;
			while(it.hasNext()){
				Result res = it.next();
				if(res.size()>0){
					long[] n = ByteTriple.parseRow(res.getRow());
					ByteTriple newBtr = new ByteTriple();
					newBtr.setPredicate(type);
					
					/*SortedBytesVLongWritable v = new SortedBytesVLongWritable(n[2]);
					Get get = new Get(v.getBytesWithPrefix());
					get.addColumn(Bytes.toBytes("2"), new byte[0]);
					res = indexTable.get(get);
					System.out.println( Bytes.toString(res.value()));*/
					
					
					newBtr.setObject(n[2]);
					ret.add(newBtr);
					procSub(newBtr, ret);
				}
			}
		}
	}

	/*
	 * ret[0] : ni join bindings for joinVar
	 * ret[1] : oi average bindings for each joinVar binding
	 */
	public double[] getStatistics (Var joinVar) throws IOException {
		double[] ret = new double[2];
		if(computedStats.containsKey(joinVar)){
			Double[] temp = computedStats.get(joinVar);
			ret[0] = temp[0];
			ret[1]= temp[1];
			return ret;
		}
		Iterator<ByteTriple> it = byteTriples.iterator();
		while(it.hasNext()){
			double[] temp = getStatisticsScan1(joinVar, it.next());
			ret[0] += temp[0];
			ret[1] +=temp[1];
		}
		if(numVars==2){
			ret[1] = ret[1]/ret[0];
		}
		else{
			ret[1]=1;
		}
		Double[] temp = new Double[2];
		temp[0]=ret[0];
		temp[1]=ret[1];
		computedStats.put(joinVar, temp);
		//System.out.println("Stats: "+joinVar+" Triple:"+bgp+" \n"+ ret[0]+" "+ret[1]);
		return ret;
	}

	public double[] getStatisticsScan1 (Var joinVar, ByteTriple byteTriple) throws IOException {
		double[] ret = new double[2];
		byte[] start = null;
		String tempTable = index+varPos.get(joinVar.toString());
		if(tempTable.startsWith("sp")){
			start = byteTriple.getSPOByte();
		}
		else if(tempTable.startsWith("so")){
			start = byteTriple.getSOPByte();
		}
		else if(tempTable.startsWith("ps")){
			start = byteTriple.getPSOByte();
		}
		else if(tempTable.startsWith("po")){
			start = byteTriple.getPOSByte();
		}
		else if(tempTable.startsWith("os")){
			start = byteTriple.getOSPByte();
		}
		else if(tempTable.startsWith("op")){
			start = byteTriple.getOPSByte();
		}
		
		if(numVars==0){
			ret[0]=1;
			ret[1]=1;
		}
		else if(numVars==1){
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			//ret = StatisticsCache.getStatistics(table,numVars,start);
			SortedMap<byte[], List<byte[]>> m = StatisticsCache.getStatistics(table,start,stop);
			double sum=0;
			for(Entry<byte[], List<byte[]>> e : m.entrySet()){
				for(byte[] b : e.getValue()){
					if(b[0]==Bytes.toBytes("S")[0]){
						byte[] b1 = new byte[b.length-2];
						System.arraycopy(b, 2, b1, 0, b1.length);
						SortedBytesVLongWritable v = new SortedBytesVLongWritable();
						v.setBytesWithPrefix(b1);
						sum += v.getLong();
					}
				}
			}
			if(sum==0)
				ret[0]=20;
			else
				ret[0]=sum;
			ret[1]=1;
		}
		else if(numVars == 2){
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			SortedMap<byte[], List<byte[]>> m = StatisticsCache.getStatistics(table,start,stop);
			double sum=0, sum1=0;
			for(Entry<byte[], List<byte[]>> e : m.entrySet()){
				for(byte[] b : e.getValue()){
					if(b[0]==Bytes.toBytes("T")[0] && b[1]==Bytes.toBytes("1")[0]){
						byte[] b1 = new byte[b.length-2];
						System.arraycopy(b, 2, b1, 0, b1.length);
						SortedBytesVLongWritable v = new SortedBytesVLongWritable();
						v.setBytesWithPrefix(b1);
						sum += v.getLong();
					}
					if(b[0]==Bytes.toBytes("T")[0] && b[1]==Bytes.toBytes("2")[0]){
						byte[] b1 = new byte[b.length-2];
						System.arraycopy(b, 2, b1, 0, b1.length);
						SortedBytesVLongWritable v = new SortedBytesVLongWritable();
						v.setBytesWithPrefix(b1);
						sum1 += v.getLong();
					}
				}
			}
			if(sum==0)
				ret[0]=20;
			else
				ret[0]=sum;

			if(sum1==0)
				ret[1]=20;
			else
				ret[1]=sum1;
			//ret[1]=sum;// /ret[0];
		}
		return ret;
	}
	public double[] getStatisticsScan (Var joinVar, ByteTriple byteTriple) throws IOException {
		double[] ret = new double[2];
		byte[] start = null;
		String tempTable = index+varPos.get(joinVar.toString());
		if(tempTable.startsWith("sp")){
			start = byteTriple.getSPOByte();
		}
		else if(tempTable.startsWith("so")){
			start = byteTriple.getSOPByte();
		}
		else if(tempTable.startsWith("ps")){
			start = byteTriple.getPSOByte();
		}
		else if(tempTable.startsWith("po")){
			start = byteTriple.getPOSByte();
		}
		else if(tempTable.startsWith("os")){
			start = byteTriple.getOSPByte();
		}
		else if(tempTable.startsWith("op")){
			start = byteTriple.getOPSByte();
		}
		
		if(numVars==0){
			ret[0]=1;
			ret[0]=1;
		}
		else if(numVars==1){
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			//System.out.println(Bytes.toStringBinary(start));
			//System.out.println(Bytes.toStringBinary(stop));
			Scan scan = new Scan(start,stop);
			scan.addFamily(Bytes.toBytes("S"));
			Iterator<Result> it = table.getScanner(scan).iterator();
			double sum=0;
			while(it.hasNext()){
				Result res = it.next();
				if(res.size()>0){
					SortedBytesVLongWritable v = new SortedBytesVLongWritable();
					v.setBytesWithPrefix(res.value());
					sum += v.getLong();
				}
			}
			if(sum==0)
				ret[0]=50;
			else
				ret[0]=sum;
			ret[1]=1;
		}
		else if(numVars == 2){
			byte[] stop = start.clone();
			stop[stop.length-1]+=(byte)1;
			//System.out.println(Bytes.toStringBinary(start));
			//System.out.println(Bytes.toStringBinary(stop));
			Scan scan = new Scan(start,stop);
			scan.addColumn(Bytes.toBytes("T"), Bytes.toBytes("1"));
			Iterator<Result> it = table.getScanner(scan).iterator();
			double sum=0;
			while(it.hasNext()){
				Result res = it.next();
				if(res.size()>0){
					SortedBytesVLongWritable v = new SortedBytesVLongWritable();
					v.setBytesWithPrefix(res.value());
					sum += v.getLong();
				}
			}
			if(sum==0)
				ret[0]=50;
			else
				ret[0]=sum;

			scan = new Scan(start,stop);
			scan.addColumn(Bytes.toBytes("T"), Bytes.toBytes("2"));
			it = table.getScanner(scan).iterator();
			sum=0;
			while(it.hasNext()){
				Result res = it.next();
				if(res.size()>0){
					SortedBytesVLongWritable v = new SortedBytesVLongWritable();
					v.setBytesWithPrefix(res.value());
					sum += v.getLong();
				}
			}
			if(sum==0)
				ret[1]=50;
			else
				ret[1]=sum;
			//ret[1]=sum;// /ret[0];
		}
		return ret;
	}
	
	public List<Scan> getScans(String joinVar){
		List<Scan> ret = new ArrayList<Scan>();
		Iterator<ByteTriple> it = byteTriples.iterator();
		while(it.hasNext()){
			ByteTriple b = it.next();
			Scan s  = getScan(joinVar, b);
			s.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table.getTableName());
			ret.add(s);
		}
		return ret;
	}
	
	
	public Scan getScan(String joinVar, ByteTriple byteTriple){
		byte[] start = null;
		Scan ret = new Scan();
		String tempTable = index+varPos.get(joinVar);
		if(tempTable.startsWith("sp")){
			start = byteTriple.getSPOByte();
			if(numVars==2){
				byte[] temp =new byte[2];
				temp[0]=varRevPos.get("p");
				temp[1]=varRevPos.get("o");
				ret.addFamily(temp);
			}
			else{
				byte[] temp =new byte[1];
				temp[0]=varRevPos.get("o");
				ret.addFamily(temp);
			}
		}
		else if(tempTable.startsWith("so")){
			start = byteTriple.getSOPByte();
			if(numVars==2){
				byte[] temp =new byte[2];
				temp[0]=varRevPos.get("o");
				temp[1]=varRevPos.get("p");
				ret.addFamily(temp);
			}
			else{
				byte[] temp =new byte[1];
				temp[0]=varRevPos.get("p");
				ret.addFamily(temp);
			}
		}
		else if(tempTable.startsWith("ps")){
			start = byteTriple.getPSOByte();
			if(numVars==2){
				byte[] temp =new byte[2];
				temp[0]=varRevPos.get("s");
				temp[1]=varRevPos.get("o");
				ret.addFamily(temp);
			}
			else{
				byte[] temp =new byte[1];
				temp[0]=varRevPos.get("o");
				ret.addFamily(temp);
			}
		}
		else if(tempTable.startsWith("po")){
			start = byteTriple.getPOSByte();
			if(numVars==2){
				byte[] temp =new byte[2];
				temp[0]=varRevPos.get("o");
				temp[1]=varRevPos.get("s");
				ret.addFamily(temp);
			}
			else{
				byte[] temp =new byte[1];
				temp[0]=varRevPos.get("s");
				ret.addFamily(temp);
			}
		}
		else if(tempTable.startsWith("os")){
			start = byteTriple.getOSPByte();
			if(numVars==2){
				byte[] temp =new byte[2];
				temp[0]=varRevPos.get("s");
				temp[1]=varRevPos.get("p");
				ret.addFamily(temp);
			}
			else{
				byte[] temp =new byte[1];
				temp[0]=varRevPos.get("p");
				ret.addFamily(temp);
			}
		}
		else if(tempTable.startsWith("op")){
			start = byteTriple.getOPSByte();
			if(numVars==2){
				byte[] temp =new byte[2];
				temp[0]=varRevPos.get("p");
				temp[1]=varRevPos.get("s");
				ret.addFamily(temp);
			}
			else{
				byte[] temp =new byte[1];
				temp[0]=varRevPos.get("s");
				ret.addFamily(temp);
			}
		}
		else if(tempTable.startsWith("o")){
			start = byteTriple.getOPSByte();
			byte[] temp =new byte[3];
			temp[0]=varRevPos.get("o");
			temp[1]=varRevPos.get("p");
			temp[2]=varRevPos.get("s");
			ret.addFamily(temp);
		}
		else if(tempTable.startsWith("p")){
			start = byteTriple.getPOSByte();
			byte[] temp =new byte[3];
			temp[0]=varRevPos.get("p");
			temp[1]=varRevPos.get("o");
			temp[2]=varRevPos.get("s");
			ret.addFamily(temp);
		}
		else if(tempTable.startsWith("s")){

			start = byteTriple.getSPOByte();
			byte[] temp =new byte[3];
			temp[0]=varRevPos.get("s");
			temp[1]=varRevPos.get("p");
			temp[2]=varRevPos.get("o");
			ret.addFamily(temp);
		}
		
		byte[] stop = start.clone();
		stop[stop.length-1]+=(byte)1;
		//System.out.println(Bytes.toStringBinary(start));
		//System.out.println(Bytes.toStringBinary(stop));
		ret.setStartRow(start);
		ret.setStopRow(stop);

		ret.setAttribute("hbase.client.scanner.seekOverhead", Bytes.toBytes((long) 1000));
		
		//ret.addFamily(Bytes.toBytes("I"));
		ret.setCaching(20000); //good for mapreduce scan
		ret.setCacheBlocks(false); //good for mapreduce scan
		ret.setBatch(20000); //good for mapreduce scan
		return ret;
	}
	
}
