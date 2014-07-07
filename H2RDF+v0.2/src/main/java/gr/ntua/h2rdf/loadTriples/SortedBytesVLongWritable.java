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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;

public class SortedBytesVLongWritable {
	  	private long value;
	  	/*
		 * 	   bytes|   prefixbyte 	| num_bits
		 *  	9	|	0000 0000	|		 60
		 *  	8	|	0000 0001	|	 	 56	
		 *  	7	|	0000 0010	|		 48	
		 *  	6	|	0000 0011	|		 40	
		 *  	5	|	0000 01--	|	32+2=34
		 *  	4	|	0000 1---	|	24+3=27 
		 *  	3	|	0001 ----	|	16+4=20  
		 *  	2	|	001- ----	|	8+5 =13
		 *  	1	|	01-- ----	|	0+6 = 6
		 *  			Positive
		 *  	1	|	10-- ----	|	0+6 = 6
		 *  	2	|	110- ----	|	8+5 =13	
		 *  	3	|	1110 ----	|	16+4=20 	
		 *  	4	|	1111 0---	|	24+3=27 	
		 *  	5	|	1111 10--	|	32+2=34 
		 *  	6	|	1111 1100	|		 40 
		 *  	7	|	1111 1101	|		 48 
		 *  	8	|	1111 1110	|	 	 56 
		 *  	9	|	1111 1111	|		 60 
		 */
		public static final byte[] POSITIVE = {
			(byte) 0x80,
			(byte) 0xc0,
			(byte) 0xe0,
			(byte) 0xf0,
			(byte) 0xf8,
			(byte) 0xfc,
			(byte) 0xfd,
			(byte) 0xfe,
			(byte) 0xff,
		}; 
		public static final byte[] NEGATIVE = {
			(byte) 0x40,
			(byte) 0x20,
			(byte) 0x10,
			(byte) 0x08,
			(byte) 0x04,
			(byte) 0x03,
			(byte) 0x02,
			(byte) 0x01,
			(byte) 0x00,
		}; 
		
		public static final byte[] MASK = {
			(byte) 0x3f,
			(byte) 0x1f,
			(byte) 0x0f,
			(byte) 0x07,
			(byte) 0x03,
			(byte) 0x00,
			(byte) 0x00,
			(byte) 0x00,
			(byte) 0x00,
		}; 
		
		public SortedBytesVLongWritable() {}

		public SortedBytesVLongWritable(long value) { set(value); }

		public void set(long value) { this.value = value; }

		public long getLong() { return value; }

		public static long readLong(InputStream is) throws IOException{
			long ret = 0;
			byte[] temp = new byte[1];
			int err = is.read(temp, 0, 1);
			if(err==-1)
				throw new EOFException();
			boolean neg =false;
			if((temp[0] & 0x80) ==0){
				neg = true;
			}
			int size=0;
			if(neg){
				int i; 
				for (i = 0; i < NEGATIVE.length; i++) {
					if((temp[0] & NEGATIVE[i]) == NEGATIVE[i]){
						break;
					}
				}
				size =i+1;
			}
			else{
				int i; 
				for (i = POSITIVE.length-1; i >=0 ; i--) {
					if((temp[0] & POSITIVE[i]) ==POSITIVE[i]){
						break;
					}
				}
				size =i+1;
			}
			byte[] temp1 = new byte[size];
			temp1[0]=temp[0];
			if(size-1>0){
				err = is.read(temp1, 1, size-1);
				if(err==-1)
					throw new EOFException();
			}
			SortedBytesVLongWritable t = new SortedBytesVLongWritable();
			t.setBytesWithPrefix(temp1);

			
			return t.getLong();
		}

		
		public byte[] getBytesWithPrefix(){
			boolean neg =false;
			long v = value;
			if(value<0){
				v ^= -1L;
				neg=true;
			}
			//System.out.println("neg: \t"+Long.toBinaryString(v));
			byte[] ret = null;
			
			int bits = 64-Long.numberOfLeadingZeros(v);
			byte[] tl = Bytes.toBytes(v);
			
			int bytes = 0;
			int offset = 0, start=0;
			if(bits<=6){
				bytes = 1;
				offset = 7;
			}
			else if(bits<=13){
				bytes = 2;
				offset = 6;
			}
			else if(bits<=20){
				bytes = 3;
				offset = 5;
			}
			else if(bits<=27){
				bytes = 4;
				offset = 4;
			}
			else if(bits<=34){
				bytes = 5;
				offset = 3;
			}
			else if(bits<=40){
				bytes = 6;
				offset = 2;
				start=1;
			}
			else if(bits<=48){
				bytes = 7;
				offset = 1;
				start=1;
			}
			else if(bits<=56){
				bytes = 8;
				offset = 0;
				start=1;
			}
			else if(bits<=64){
				bytes = 9;
				offset = -1;
				start=1;
			}
			//System.out.println("bits: "+bits+" bytes: "+ bytes+" start: "+start+ " offset: "+offset);
			ret = new byte[bytes];
			for (int i = start; i < bytes; i++) {
				ret[i] = tl[offset+i];
			}
			if(neg){
				ret[0]^=NEGATIVE[bytes-1];
			}
			else{
				ret[0]^=POSITIVE[bytes-1];
			}
			return ret;
			
		}
	  

		public void setBytesWithPrefix(byte[] v) {
			boolean neg =false;
			byte[] temp = new byte[v.length];
			for (int i = 0; i < temp.length; i++) {
				temp[i]=v[i];
			}
			byte[] l = new byte[8]; 
			if((temp[0] & 0x80) ==0){
				neg = true;
			}
			//remove header
			temp[0]&=MASK[temp.length-1];
			
			int offset = 8-temp.length;
			int start=0;
			if(temp.length>=6){
				start=1;
			}
			
			for (int i = start; i < temp.length; i++) {
				l[offset+i] = temp[i];
			}
			//System.out.println("decode:\t"+Long.toBinaryString(Bytes.toLong(l)));
				
			long val = Bytes.toLong(l);
			if(neg){
				val ^= -1L;
			}
			this.value = val;
		}
	  
		public static void main(String[] args)
	    {
			Random r = new Random();
			double count =1;
			long max = Long.MAX_VALUE;
			while (count<=max) {
				long l = r.nextLong();
				l=Math.round(((double)l)/count);
				//System.out.println("rand: "+l+" \nencode:\t"+Long.toBinaryString(l));
				SortedBytesVLongWritable t = new SortedBytesVLongWritable(l);
				SortedBytesVLongWritable t1 = new SortedBytesVLongWritable();
		    	byte[] temp = t.getBytesWithPrefix();
		    	t1.setBytesWithPrefix(temp);
				//t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
				if(l!=t1.getLong()){
					System.out.println("error "+Long.toHexString(t1.getLong()));
					break;
				}
				System.out.println(" bytes: "+temp.length);
				count*=1.000001;
			}
	    	/*long l= Long.MIN_VALUE;
	    	BytesVLongWritable t = new BytesVLongWritable(l);
	    	BytesVLongWritable t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-7334454566668888885");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-5334454566668888");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-13344545666456");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-133445456666");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-34454565");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-445455");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-4545");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-54");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("-1");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("0");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("5455");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("454566");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("44545665");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("34454566665");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("334454566665");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= new Long("13344545666675565");
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);
	    	l= Long.MAX_VALUE;
	    	t = new BytesVLongWritable(l);
	    	t1 = new BytesVLongWritable();
			t1.setBytesWithOutPrefix(t.getBytesWithOutPrefix());
	    	System.out.println(l+" \t"+Long.toBinaryString(t.getLong())+"\t\t"+t1.getLong()+"\t"+t.getBytesWithOutPrefix().length);*/
	    }

		public static long readLong(DataInput in) throws IOException{
			long ret = 0;
			byte[] temp = new byte[1];
			temp[0] = in.readByte();
			/*int err = is.read(temp, 0, 1);
			if(err==-1)
				throw new EOFException();*/
			boolean neg =false;
			if((temp[0] & 0x80) ==0){
				neg = true;
			}
			int size=0;
			if(neg){
				int i; 
				for (i = 0; i < NEGATIVE.length; i++) {
					if((temp[0] & NEGATIVE[i]) == NEGATIVE[i]){
						break;
					}
				}
				size =i+1;
			}
			else{
				int i; 
				for (i = POSITIVE.length-1; i >=0 ; i--) {
					if((temp[0] & POSITIVE[i]) ==POSITIVE[i]){
						break;
					}
				}
				size =i+1;
			}
			byte[] temp1 = new byte[size];
			temp1[0]=temp[0];
			if(size-1>0){
				in.readFully(temp1, 1, size-1);
				
				/*err = is.read(temp1, 1, size-1);
				if(err==-1)
					throw new EOFException();*/
			}
			SortedBytesVLongWritable t = new SortedBytesVLongWritable();
			t.setBytesWithPrefix(temp1);

			
			return t.getLong();
		}


}
