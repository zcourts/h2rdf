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

import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Bindings extends BytesWritable {
	public Map<Byte, Set<Long> > map;
	private int size;
	public byte pattern;
	private HashMap<Integer, Integer> varRelabeling;
	private HashMap<Integer, Long> selectiveBindings;
	public boolean valid;
	
	public Bindings() {
		map = new HashMap<Byte, Set<Long>>();
		this.varRelabeling=null;
		this.selectiveBindings=null;
	}
	
	public Bindings(HashMap<Integer, Integer> varRelabeling) {
		map = new HashMap<Byte, Set<Long>>();
		this.varRelabeling=varRelabeling;
		this.selectiveBindings=null;
	}


	public Bindings(HashMap<Integer, Integer> varRelabeling,
			HashMap<Integer, Long> selectiveBindings) {
		map = new HashMap<Byte, Set<Long>>();
		this.varRelabeling=varRelabeling;
		this.selectiveBindings=selectiveBindings;
	}

	public Bindings(HashMap<Integer, Long> selectiveBindings, int i) {
		map = new HashMap<Byte, Set<Long>>();
		this.selectiveBindings=selectiveBindings;
		this.varRelabeling=null;
	}

	public void setPattern(byte b){
		this.pattern = b;
	}
	
	public void addBinding(byte varId, long value){
		Set<Long> val = map.get(varId);
		if(val==null){
			val = new HashSet<Long>();
			val.add(value);
			map.put(varId, val);
		}
		else{
			val.add(value);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		map = new HashMap<Byte, Set<Long>>();
		this.pattern = in.readByte();
		long size = SortedBytesVLongWritable.readLong(in);
		valid=true;
		for (long i = 0; i < size; i++) {
			byte b = in.readByte();
			Integer v = (int)b;
			if(selectiveBindings!=null) {
				if(varRelabeling!=null){//relabel v
					v=varRelabeling.get(v);
				}
				if(selectiveBindings.containsKey(v)){
					//selection
					long s = SortedBytesVLongWritable.readLong(in);
					long selectiveId = selectiveBindings.get(v);
					Set<Long> l=new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						long tl = SortedBytesVLongWritable.readLong(in);
						if(tl==selectiveId){
							l.add(tl);
						}
					}
					if(l.size()==0){
						valid=false;
					}
				}
				else{
					long s = SortedBytesVLongWritable.readLong(in);
					Set<Long> l = new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						l.add(SortedBytesVLongWritable.readLong(in));
					}
					map.put((byte)(int)v, l);
				}
			}
			else{
				long s = SortedBytesVLongWritable.readLong(in);
				Set<Long> l = new HashSet<Long>();
				for (int j = 0; j < s; j++) {
					l.add(SortedBytesVLongWritable.readLong(in));
				}
				if(varRelabeling!=null){
					map.put((byte)(int)varRelabeling.get(v), l);
				}
				else{
					map.put(b, l);
				}
			}
		}
	}

	public void readFields(byte[] bin) throws IOException {
		map = new HashMap<Byte, Set<Long>>();
		this.pattern = bin[0];
		ByteArrayInputStream in = new ByteArrayInputStream(bin);

		valid=true;
		byte[] b1 =new byte[1];
		in.read(b1, 0, 1);

		long size = SortedBytesVLongWritable.readLong(in);
		for (long i = 0; i < size; i++) {
			byte[] b = new byte[1];
			in.read(b, 0, 1);
			long s = SortedBytesVLongWritable.readLong(in);
			Set<Long> l = new HashSet<Long>();
			for (int j = 0; j < s; j++) {
				l.add(SortedBytesVLongWritable.readLong(in));
			}
			if(varRelabeling!=null){
				Integer v = (int)b[0];
				map.put((byte)(int)varRelabeling.get(v), l);
			}
			else{
				map.put(b[0], l);
			}
		}
	}

	public void readFields(InputStream in, HashMap<Integer, Integer> varRelabeling, HashMap<Integer, Long> selectiveBindings) throws IOException {
		map = new HashMap<Byte, Set<Long>>();

		valid=true;
		byte[] b1 =new byte[1];
		in.read(b1, 0, 1);

		long size = SortedBytesVLongWritable.readLong(in);
		for (long i = 0; i < size; i++) {
			byte[] b = new byte[1];
			in.read(b, 0, 1);
			int v = (int)b[0];
			if(selectiveBindings!=null){
				if(varRelabeling!=null){//relabel v
					v=varRelabeling.get(v);
				}
				if(selectiveBindings.containsKey(v)){
					//selection
					long s = SortedBytesVLongWritable.readLong(in);
					long selectiveId = selectiveBindings.get(v);
					//System.out.print(v+","+selectiveId+" : ");
					Set<Long> l=new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						long tl = SortedBytesVLongWritable.readLong(in);
						//System.out.print(tl+",");
						if(tl==selectiveId){
							l.add(tl);
						}
					}
					if(l.size()==0){
						valid=false;
					}
					//System.out.println();
				}
				else{
					long s = SortedBytesVLongWritable.readLong(in);
					Set<Long> l = new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						l.add(SortedBytesVLongWritable.readLong(in));
					}
					map.put((byte)(int)v, l);
				}
			}
			else{
				long s = SortedBytesVLongWritable.readLong(in);
				Set<Long> l = new HashSet<Long>();
				for (int j = 0; j < s; j++) {
					l.add(SortedBytesVLongWritable.readLong(in));
				}
				if(varRelabeling!=null){
					map.put((byte)(int)varRelabeling.get(v), l);
				}
				else{
					map.put(b[0], l);
				}
			}
		}
		
	}
	
	public void readFields(byte[] bin, HashMap<Integer, Integer> varRelabeling, HashMap<Integer, Long> selectiveBindings) throws IOException {
		map = new HashMap<Byte, Set<Long>>();
		this.pattern = bin[0];
		ByteArrayInputStream in = new ByteArrayInputStream(bin);

		valid=true;
		byte[] b1 =new byte[1];
		in.read(b1, 0, 1);

		long size = SortedBytesVLongWritable.readLong(in);
		for (long i = 0; i < size; i++) {
			byte[] b = new byte[1];
			in.read(b, 0, 1);
			int v = (int)b[0];
			if(selectiveBindings!=null){
				if(varRelabeling!=null){//relabel v
					v=varRelabeling.get(v);
				}
				if(selectiveBindings.containsKey(v)){
					//selection
					long s = SortedBytesVLongWritable.readLong(in);
					long selectiveId = selectiveBindings.get(v);
					//System.out.print(v+","+selectiveId+" : ");
					Set<Long> l=new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						long tl = SortedBytesVLongWritable.readLong(in);
						//System.out.print(tl+",");
						if(tl==selectiveId){
							l.add(tl);
						}
					}
					if(l.size()==0){
						valid=false;
					}
					//System.out.println();
				}
				else{
					long s = SortedBytesVLongWritable.readLong(in);
					Set<Long> l = new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						l.add(SortedBytesVLongWritable.readLong(in));
					}
					map.put((byte)(int)v, l);
				}
			}
			else{
				long s = SortedBytesVLongWritable.readLong(in);
				Set<Long> l = new HashSet<Long>();
				for (int j = 0; j < s; j++) {
					l.add(SortedBytesVLongWritable.readLong(in));
				}
				if(varRelabeling!=null){
					map.put((byte)(int)varRelabeling.get(v), l);
				}
				else{
					map.put(b[0], l);
				}
			}
		}
		
	}
	
	public void readFields(InputStream in) throws IOException {
		map = new HashMap<Byte, Set<Long>>();
		valid=true;
		byte[] b1 =new byte[1];
		in.read(b1, 0, 1);
		this.pattern=b1[0];
		
		long size = SortedBytesVLongWritable.readLong(in);
		for (long i = 0; i < size; i++) {
			byte[] b = new byte[1];
			in.read(b, 0, 1);
			int v = (int)b[0];
			if(selectiveBindings!=null){
				if(varRelabeling!=null){//relabel v
					v=varRelabeling.get(v);
				}
				if(selectiveBindings.containsKey(v)){
					//selection
					long s = SortedBytesVLongWritable.readLong(in);
					long selectiveId = selectiveBindings.get(v);
					//System.out.print(v+","+selectiveId+" : ");
					Set<Long> l=new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						long tl = SortedBytesVLongWritable.readLong(in);
						//System.out.print(tl+",");
						if(tl==selectiveId){
							l.add(tl);
						}
					}
					if(l.size()==0){
						valid=false;
					}
					//System.out.println();
				}
				else{
					long s = SortedBytesVLongWritable.readLong(in);
					Set<Long> l = new HashSet<Long>();
					for (int j = 0; j < s; j++) {
						l.add(SortedBytesVLongWritable.readLong(in));
					}
					map.put((byte)(int)v, l);
				}
			}
			else{
				long s = SortedBytesVLongWritable.readLong(in);
				Set<Long> l = new HashSet<Long>();
				for (int j = 0; j < s; j++) {
					l.add(SortedBytesVLongWritable.readLong(in));
				}
				if(varRelabeling!=null){
					map.put((byte)(int)varRelabeling.get(v), l);
				}
				else{
					map.put(b[0], l);
				}
			}
		}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(pattern);
		SortedBytesVLongWritable v = new SortedBytesVLongWritable(map.size());
		out.write(v.getBytesWithPrefix());
		for(Entry<Byte, Set<Long> > e : map.entrySet()){
			out.write(e.getKey());
			v = new SortedBytesVLongWritable(e.getValue().size());
			out.write(v.getBytesWithPrefix());
			for(Long l : e.getValue()){
				v = new SortedBytesVLongWritable(l);
				out.write(v.getBytesWithPrefix());
			}
		}
	}
	
	public void writeWithoutPattern(DataOutput out) throws IOException {
		SortedBytesVLongWritable v = new SortedBytesVLongWritable(map.size());
		out.write(v.getBytesWithPrefix());
		for(Entry<Byte, Set<Long> > e : map.entrySet()){
			out.write(e.getKey());
			v = new SortedBytesVLongWritable(e.getValue().size());
			out.write(v.getBytesWithPrefix());
			for(Long l : e.getValue()){
				v = new SortedBytesVLongWritable(l);
				out.write(v.getBytesWithPrefix());
			}
		}
	}

	@Override
	public byte[] getBytes() {
		ByteArrayOutputStream b =new ByteArrayOutputStream();
		DataOutput d = new DataOutputStream(b);
		try {
			write(d);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		size = b.size();
		return b.toByteArray();
	}

	@Override
	public int getLength() {
		return size;
	}

	public void addAll(Bindings b) {
		for(Entry<Byte, Set<Long> > e : b.map.entrySet()){
			Set<Long> val = map.get(e.getKey());
			if(val == null){
				map.put(e.getKey(), e.getValue());
			}
			else{
				val.addAll(e.getValue());
			}
		}
		
	}

	public static List<Bindings> merge(List<Bindings> l1, List<Bindings> l2) {
		/*Set<Byte> doubleVar = new HashSet<Byte>();
		Set<Byte> nonDoubleVar = new HashSet<Byte>();
		Set<Byte> s1 = l1.get(0).map.keySet();
		Set<Byte> s2 = l2.get(0).map.keySet();
		for (Byte b : s1) {
			//System.out.println(" Var1: "+b);
			if(s2.contains(b)){
				//System.out.println("Double Var: "+b);
				doubleVar.add(b);
			}
			else
				nonDoubleVar.add(b);
		}
		for (Byte b : s2) {
			//System.out.println(" Var2: "+b);
			if(!doubleVar.contains(b))
				nonDoubleVar.add(b);
		}
		
		List<Bindings> ret = new ArrayList<Bindings>();
		//System.out.println("Merging: "+l1.size()+" "+l2.size());
		for(Bindings b1 : l2){
			for(Bindings b2 : l1){
				if(doubleVar.size()==0){
					Bindings b = b1.clone();
					b.map.putAll(b2.map);
					ret.add(b);
				}
				else{
					Bindings bb = new Bindings();
					boolean notFound=false;
					for(Byte b : doubleVar){
						Set<Long> newv = new HashSet<Long>();
						Set<Long> v1 = b1.map.get(b);
						Set<Long> v2 = b2.map.get(b);
						if(v1.size()>v2.size()){
							for(Long l : v2){
								if(v1.contains(l))
									newv.add(l);
							}
						}
						else{
							for(Long l : v1){
								if(v2.contains(l))
									newv.add(l);
							}
						}
						
						if(newv.isEmpty()){
							notFound=true;
							break;
						}
						bb.map.put(b, newv);
					}
					if(!notFound){
						for(Byte b : nonDoubleVar){
							Set<Long> temp = b1.map.get(b);
							if(temp!=null)
								bb.map.put(b, temp);
							else{
								temp = b2.map.get(b);
								if(temp!=null)
									bb.map.put(b, temp);
							}
						}
						ret.add(bb);
					}
				}
				//Bindings b = merge(b1,b2);
				//if(b!=null)
				//	ret.add(b);
			}
		}*/
		List<Bindings> ret = new ArrayList<Bindings>();
		//System.out.println("Merging: "+l1.size()+" "+l2.size());
		for(Bindings b1 : l2){
			for(Bindings b2 : l1){
				Bindings b = merge(b1,b2);
				if(b!=null)
					ret.add(b);
			}
		}
		return ret;
		
	}

	private static Bindings merge(Bindings b1, Bindings b2) {
		//Bindings ret = new Bindings();
		//ret.map = b1.map;
		
		Bindings ret = b1.clone();
		Map<Byte, Set<Long>> m1 = b1.map;
		Map<Byte, Set<Long>> m2 = b2.map;
		Set<Byte> attributes = new HashSet<Byte>();
		for(Entry<Byte, Set<Long>> e1 : m1.entrySet()){
			attributes.add(e1.getKey());
		}
		for(Entry<Byte, Set<Long>> e2 : m2.entrySet()){
			attributes.add(e2.getKey());
			Set<Long> val = ret.map.remove(e2.getKey());
			if(val==null){
				ret.map.put(e2.getKey(), e2.getValue());
			}
			else{ //double var
				Set<Long> temp = new HashSet<Long>();
				for(Long l : e2.getValue()){
					if(val.contains(l))
						temp.add(l);
				}
				if(temp.size()>0){
					ret.map.put(e2.getKey(), temp);
				}
			}
		}
		if(ret.map.size()==attributes.size())
			return ret;
		else
			return null;
	}


	public static void mergeSamePattern(List<Bindings> bin, Bindings b) {
		if(b.map.size()==1){
			bin.get(0).addAll(b);
		}
		else{
			bin.add(b);
		}
	}
	
	public Bindings clone(){
		Bindings ret = new Bindings();
		for(Entry<Byte, Set<Long>> e : map.entrySet()){
			Set<Long> l = new HashSet<Long>();
			l.addAll(e.getValue());
			ret.map.put(e.getKey(), l);
		}
		ret.pattern = pattern;
		ret.size =size;
		return ret;
		
	}

	public void print(HTable indexTable) throws IOException {
		for(Entry<Byte, Set<Long>> e : map.entrySet()){
			System.out.print("["+e.getKey()+":");
			for (Long l : e.getValue()){
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(l);
				Get get = new Get(v.getBytesWithPrefix());
				get.addColumn(Bytes.toBytes("2"), new byte[0]);
				Result res = indexTable.get(get);
				System.out.print(Bytes.toString(res.value())+",");
			}
			System.out.print("] ");
		}
		System.out.println();
		
	}

	public void writeOut(Context context, int varPos, Bindings currBinding) throws IOException, InterruptedException {
		Byte[] keys = new Byte[map.keySet().size()];
		keys = map.keySet().toArray(keys);
		if(varPos>= keys.length){
			//System.out.println("Output: "+currBinding.map);
			context.write(currBinding, new BytesWritable(new byte[0]));
			return;
		}
			
		int count=0;
		Iterator<Long> it = map.get(keys[varPos]).iterator();
		Set<Long> tempSet = new HashSet<Long>();
		while(it.hasNext()){
			Long val = it.next();
			tempSet.add(val);
			count++;
			if(count>=100){
				count=0;
				Bindings temp = currBinding.clone();
				temp.map.put(keys[varPos], tempSet);
				writeOut(context, varPos+1, temp);
				tempSet = new HashSet<Long>();
			}
			
		}
		if(count>0){
			count=0;
			Bindings temp = currBinding.clone();
			temp.map.put(keys[varPos], tempSet);
			writeOut(context, varPos+1, temp);
		}
		
		
		/*List<Bindings> output= new ArrayList<Bindings>();
		output.add(new Bindings());
		while(!map.isEmpty()){
			Byte v = map.keySet().iterator().next();
			Iterator<Long> it = map.get(v).iterator();
			int count=0;
			Set<Long> tempSet = new HashSet<Long>();
			List<Bindings> temp= new ArrayList<Bindings>();
			while(it.hasNext()){
				Long val = it.next();
				tempSet.add(val);
				count++;
				if(count>=100){
					count=0;
					for(Bindings b : output){
						Bindings bt = new Bindings();
						bt.addAll(b);
						bt.map.put(v, tempSet);
						temp.add(bt);
					}
					tempSet = new HashSet<Long>();
				}
			}
			if(count>0){
				count=0;
				for(Bindings b : output){
					Bindings bt = new Bindings();
					bt.addAll(b);
					bt.map.put(v, tempSet);
					temp.add(bt);
				}
				tempSet = new HashSet<Long>();
			}
			output=temp;
			map.remove(v);
		}
		
		for (Bindings b : output) {
			//System.out.println("Output: "+b.map);
			context.write(b, new BytesWritable(new byte[0]));
		}*/
	}
	
	public static void main(String[] args) {
		Bindings b = new Bindings();
		Set<Long> set = new HashSet<Long>();
		set.add(new Long(0));
		set.add(new Long(2));
		set.add(new Long(4));
		set.add(new Long(5));
		set.add(new Long(7));
		set.add(new Long(14));
		set.add(new Long(16));
		set.add(new Long(19));
		b.map.put((byte) 0 , set);
		
		set = new HashSet<Long>();
		set.add(new Long(10));
		set.add(new Long(12));
		set.add(new Long(14));
		set.add(new Long(15));
		set.add(new Long(17));
		set.add(new Long(114));
		set.add(new Long(116));
		set.add(new Long(119));
		b.map.put((byte) 4 , set);
		
		set = new HashSet<Long>();
		set.add(new Long(20));
		set.add(new Long(22));
		set.add(new Long(24));
		set.add(new Long(25));
		set.add(new Long(27));
		set.add(new Long(214));
		set.add(new Long(216));
		set.add(new Long(219));
		b.map.put((byte) 7 , set);
		
		try {
			b.writeOut(null, 0, new Bindings());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
