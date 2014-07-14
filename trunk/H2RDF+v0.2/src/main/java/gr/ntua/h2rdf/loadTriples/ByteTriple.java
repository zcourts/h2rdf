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
package gr.ntua.h2rdf.loadTriples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ByteTriple {
	private long s,p,o;
	private byte[] sub, pred, obj;

	public ByteTriple() {
		s=0;
		p=0;
		o=0;
		sub = new byte[0];
		pred = new byte[0];
		obj = new byte[0];
	}
	
	public ByteTriple(long s, long p, long o) {
		super();
		this.s = s;
		this.p = p;
		this.o = o;
		this.sub = (new SortedBytesVLongWritable(s)).getBytesWithPrefix();
		this.pred = (new SortedBytesVLongWritable(p)).getBytesWithPrefix();
		this.obj = (new SortedBytesVLongWritable(o)).getBytesWithPrefix();
	}

	public ByteTriple(SortedBytesVLongWritable sub,
			SortedBytesVLongWritable pred, SortedBytesVLongWritable obj) {
		super();
		this.sub = sub.getBytesWithPrefix();
		this.pred = pred.getBytesWithPrefix();
		this.obj = obj.getBytesWithPrefix();
	}

	public static byte[] createByte(long l1, long l2, long l3, byte table) {
		SortedBytesVLongWritable s = new SortedBytesVLongWritable(l1);
		byte[] lb1= s.getBytesWithPrefix();
		s = new SortedBytesVLongWritable(l2);
		byte[] lb2= s.getBytesWithPrefix();
		s = new SortedBytesVLongWritable(l3);
		byte[] lb3= s.getBytesWithPrefix();
		
		byte[] ret = new byte[1+lb1.length+lb2.length+lb3.length];
		ret[0] = table;
		System.arraycopy(lb1, 0, ret, 1, lb1.length);
		System.arraycopy(lb2, 0, ret, lb1.length+1, lb2.length);
		System.arraycopy(lb3, 0, ret, lb1.length+lb2.length+1, lb3.length);
		//System.arraycopy(id, 0, ret, lb2.length+lb1.length+1, id.length);
		return ret;
	}
	
	public static byte[] createByte(long l1, long l2, byte table) {
		SortedBytesVLongWritable s = new SortedBytesVLongWritable(l1);
		byte[] lb1= s.getBytesWithPrefix();
		s = new SortedBytesVLongWritable(l2);
		byte[] lb2= s.getBytesWithPrefix();
		
		byte[] ret = new byte[1+lb1.length+lb2.length];
		ret[0] = table;
		System.arraycopy(lb1, 0, ret, 1, lb1.length);
		System.arraycopy(lb2, 0, ret, lb1.length+1, lb2.length);
		//System.arraycopy(id, 0, ret, lb2.length+lb1.length+1, id.length);
		return ret;
		
	}
	
	public static byte[] createByte(long l1, byte table) {
		SortedBytesVLongWritable s = new SortedBytesVLongWritable(l1);
		byte[] lb1= s.getBytesWithPrefix();
		byte[] ret = new byte[1+lb1.length];
		ret[0] = table;
		System.arraycopy(lb1, 0, ret, 1, lb1.length);
		//System.arraycopy(id, 0, ret, lb1.length+1, id.length);
		return ret;
		
	}
	
	public static long[] parseRow(byte[] row) throws IOException {
		long[] ret = new long[3];

		InputStream is = new ByteArrayInputStream(row,1,row.length-1);
		ret[0] = SortedBytesVLongWritable.readLong(is);
		ret[1] = SortedBytesVLongWritable.readLong(is);
		ret[2] = SortedBytesVLongWritable.readLong(is);
		
		return ret;
	}

	public long getS() {
		return s;
	}

	public void setSubject(long s) {
		this.s = s;
		sub = new SortedBytesVLongWritable(s).getBytesWithPrefix();
	}

	public long getP() {
		return p;
	}

	public void setPredicate(long p) {
		this.p = p;
		pred = new SortedBytesVLongWritable(p).getBytesWithPrefix();
	}

	public long getO() {
		return o;
	}

	public void setObject(long o) {
		this.o = o;
		obj = new SortedBytesVLongWritable(o).getBytesWithPrefix();
	}

	public byte[] getSub() {
		return sub;
	}

	public void setSub(SortedBytesVLongWritable sub) {
		this.sub = sub.getBytesWithPrefix();
		this.s=sub.getLong();
	}

	public byte[] getPred() {
		return pred;
	}

	public void setPred(SortedBytesVLongWritable pred) {
		this.pred = pred.getBytesWithPrefix();
		this.p = pred.getLong();
	}

	public byte[] getObj() {
		return obj;
	}

	public void setObj(SortedBytesVLongWritable obj) {
		this.obj = obj.getBytesWithPrefix();
		this.o = obj.getLong();
	}

	public byte[] getSPOByte() {
		byte[] ret = new byte[1+sub.length+obj.length+pred.length];
		ret[0]=(byte)1;
		System.arraycopy(sub, 0, ret, 1, sub.length);
		System.arraycopy(pred, 0, ret, sub.length+1, pred.length);
		System.arraycopy(obj, 0, ret, sub.length+pred.length+1, obj.length);
		return ret;
	}

	public byte[] getSOPByte() {
		byte[] ret = new byte[1+sub.length+obj.length+pred.length];
		ret[0]=(byte)2;
		System.arraycopy(sub, 0, ret, 1, sub.length);
		System.arraycopy(obj, 0, ret, sub.length+1, obj.length);
		System.arraycopy(pred, 0, ret, sub.length+obj.length+1, pred.length);
		return ret;
	}

	public byte[] getPSOByte() {
		byte[] ret = new byte[1+sub.length+obj.length+pred.length];
		ret[0]=(byte)3;
		System.arraycopy(pred, 0, ret, 1, pred.length);
		System.arraycopy(sub, 0, ret, pred.length+1, sub.length);
		System.arraycopy(obj, 0, ret, pred.length+sub.length+1, obj.length);
		return ret;
	}

	public byte[] getPOSByte() {
		byte[] ret = new byte[1+sub.length+obj.length+pred.length];
		ret[0]=(byte)4;
		System.arraycopy(pred, 0, ret, 1, pred.length);
		System.arraycopy(obj, 0, ret, pred.length+1, obj.length);
		System.arraycopy(sub, 0, ret, pred.length+obj.length+1, sub.length);
		return ret;
	}

	public byte[] getOPSByte() {
		byte[] ret = new byte[1+sub.length+obj.length+pred.length];
		ret[0]=(byte)5;
		System.arraycopy(obj, 0, ret, 1, obj.length);
		System.arraycopy(pred, 0, ret, obj.length+1, pred.length);
		System.arraycopy(sub, 0, ret, obj.length+pred.length+1, sub.length);
		return ret;
	}

	public byte[] getOSPByte() {
		byte[] ret = new byte[1+sub.length+obj.length+pred.length];
		ret[0]=(byte)6;
		System.arraycopy(obj, 0, ret, 1, obj.length);
		System.arraycopy(sub, 0, ret, obj.length+1, sub.length);
		System.arraycopy(pred, 0, ret, obj.length+sub.length+1, pred.length);
		return ret;
	}

	public byte[] getOSByte() {
		// TODO Auto-generated method stub
		return null;
	}
	

	public byte[] getOPByte() {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] getPOByte() {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] getPSByte() {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] getSPByte() {
		// TODO Auto-generated method stub
		return null;
	}

	public byte[] getSOByte() {
		// TODO Auto-generated method stub
		return null;
	}


}
