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
package gr.ntua.h2rdf.bytes;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;



public class ByteValues {
	
	public static final int totalBytes = 9;
	public static final int typeBytes = 1;
	public static final int valueBytes = 8;
	
	public static final byte  TYPE_STRING= (byte)1;
	public static final byte  TYPE_BOOLEAN=(byte)2;
	public static final byte  TYPE_INT=(byte)3;
	public static final byte  TYPE_LONG=(byte)4;
	public static final byte  TYPE_DEMICAL=(byte)5;
	public static final byte  TYPE_FLOAT=(byte)6;
	public static final byte  TYPE_DOUBLE=(byte)7;
	public static final byte  TYPE_DURATION=(byte)8;
	public static final byte  TYPE_DATETIME=(byte)9;
	public static final byte  TYPE_TIME=(byte)10;
	public static final byte  TYPE_DATE=(byte)11;
	
	public static byte[] getFullValue(String id) throws NotSupportedDatatypeException{
		byte[] ret= new byte[totalBytes];

		byte t =(byte) 1;//default String
		byte[] val = null;
		if(id.contains("\"^^")){//type
			StringTokenizer tokenizer = new StringTokenizer(id);
			String value = tokenizer.nextToken("^^");
			String type = tokenizer.nextToken();
			type = type.substring(type.lastIndexOf("#")+1, type.length()-1);
			//System.out.println(type +" value: "+value);
			if(type.contains("string")){
				t=TYPE_STRING;
				//System.out.println(t +" value: "+value);
				MD5Hash md5h = MD5Hash.digest(value);
				long hashVal = md5h.halfDigest();
				val = Bytes.toBytes(hashVal);
			}
			else if(type.contains("boolean")){
				t=TYPE_BOOLEAN;
				val = new byte[valueBytes];
				if(value.contains("true") || value.contains("1")){
					val[valueBytes-1]= (byte) 1;
				}
				else if(value.contains("false") || value.contains("0")){
					val[valueBytes-1]= (byte) 0;
				}
			}
			else if(type.contains("int")){
				t=TYPE_INT;
				val = Bytes.toBytes(SortedInt.
						toSortedInt(Integer.parseInt(value.substring(1, value.length()-1))));
			}
			else if(type.contains("long")){
				t=TYPE_LONG;
				val = Bytes.toBytes(SortedLong.
						toSortedLong(Long.parseLong(value.substring(1, value.length()-1))));
			}
			else if(type.contains("demical")){
				t=TYPE_DEMICAL;
				throw new NotSupportedDatatypeException(type);
			}
			else if(type.contains("float")){
				t=TYPE_FLOAT;
				val = Bytes.toBytes(Float.parseFloat(value.substring(1, value.length()-1)));
			}
			else if(type.contains("double")){
				t=TYPE_DOUBLE;
				val = Bytes.toBytes(Double.parseDouble(value.substring(1, value.length()-1)));
			}
			else if(type.contains("duration")){
				t=TYPE_DURATION;
				throw new NotSupportedDatatypeException(type);
			}
			else if(type.contains("datetime")){
				t=TYPE_DATETIME;
				throw new NotSupportedDatatypeException(type);
			}
			else if(type.contains("time")){
				t=TYPE_TIME;
				throw new NotSupportedDatatypeException(type);
			}
			else if(type.contains("date")){
				t=TYPE_DATE;
				System.out.println(type +" value: "+value);
				String date = value.substring(1, value.length()-1);
				StringTokenizer t2 = new StringTokenizer(date);
				int day = Integer.parseInt(t2.nextToken("/"));
				int month = Integer.parseInt(t2.nextToken("/"));
				int year = Integer.parseInt(t2.nextToken("/"));
				System.out.println(day+" "+month+" "+year);
				
				Calendar c = Calendar.getInstance();
				c.set(year, month, day);
				val = Bytes.toBytes(c.getTimeInMillis());
			}
			else {
				System.out.println(type +" value: "+value);
				throw new NotSupportedDatatypeException(type);
			}
			
			
		}
		else{//string
			t=TYPE_STRING;
			//System.out.println(t +" value: "+value);
			MD5Hash md5h = MD5Hash.digest(id);
			long hashVal = md5h.halfDigest();
			val = Bytes.toBytes(hashVal);
		}
		
		ret[0]=t;
		for (int i = 0; i < valueBytes; i++) {
			ret[i+1]=val[i];
		}
		
		if (ret.length!=totalBytes){
			System.exit(5);
		}
		
		return ret;
		
	}
	
	public static String getStringValue(byte[] id) throws NotSupportedDatatypeException{
		String ret = "";
		byte[] l = new byte[valueBytes];
		for (int i = 0; i < l.length; i++) {
			l[i]=id[i+1];
		}
		switch (id[0]) {
		case TYPE_STRING:
			ret+=id[0]+"|"+Bytes.toLong(l);
			break;
		case TYPE_BOOLEAN:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_INT:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_LONG:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_DEMICAL:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_FLOAT:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_DOUBLE:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_DURATION:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_DATETIME:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_TIME:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_DATE:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		default:
			throw new NotSupportedDatatypeException(id[0]+"");
		}
		return ret;
	}

	public static String translate(byte type, byte[] value, HTable table) throws NotSupportedDatatypeException {
		String ret = "";
		switch (type) {
		case TYPE_STRING:
			byte[] temp1=new byte[ByteValues.totalBytes];
			temp1[0]=type;
			for (int i = 0; i < ByteValues.totalBytes-1; i++) {
				temp1[i+1]=value[i];
			}
			//byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			byte[] k = new byte[ByteValues.totalBytes+1];
			k[0]=(byte) 1;
			
			for (int j = 0; j < ByteValues.totalBytes; j++) {
				k[j+1]=temp1[j];
			}
			Get get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					ret+=Bytes.toString(result.raw()[0].getValue());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		case TYPE_BOOLEAN:
			if(value[valueBytes-1]==(byte) 1)
				ret+=true;
			else
				ret+=false;
			
			break;
		case TYPE_INT:
			ret+=SortedInt.toInt(Bytes.toLong(value));
			
			break;
		case TYPE_LONG:
			ret+=SortedLong.toLong(Bytes.toLong(value));
			
			break;
		case TYPE_DEMICAL:
			throw new NotSupportedDatatypeException(type+"");
			
			//break;
		case TYPE_FLOAT:
			ret+=Bytes.toFloat(value);
			
			break;
		case TYPE_DOUBLE:
			ret+=Bytes.toDouble(value);
			
			break;
		case TYPE_DURATION:
			throw new NotSupportedDatatypeException(type+"");
			
			//break;
		case TYPE_DATETIME:
			throw new NotSupportedDatatypeException(type+"");
			
			//break;
		case TYPE_TIME:
			throw new NotSupportedDatatypeException(type+"");
			
			//break;
		case TYPE_DATE:
			throw new NotSupportedDatatypeException(type+"");
			
			//break;
		default:
			throw new NotSupportedDatatypeException(type+"");
		}
		return ret;
	}
		
	public static byte[] getFullValue(String value,
			String type) throws NotSupportedDatatypeException{
		byte[] ret= new byte[totalBytes];
		value="\""+value+"\"";
		//System.out.println(value);
		byte t =(byte) 1;//default String
		byte[] val = null;
		if(type.contains("string")){
			t=TYPE_STRING;
			//System.out.println(t +" value: "+value);
			MD5Hash md5h = MD5Hash.digest(value);
			long hashVal = md5h.halfDigest();
			val = Bytes.toBytes(hashVal);
		}
		else if(type.contains("boolean")){
			t=TYPE_BOOLEAN;
			val = new byte[valueBytes];
			if(value.contains("true") || value.contains("1")){
				val[valueBytes-1]= (byte) 1;
			}
			else if(value.contains("false") || value.contains("0")){
				val[valueBytes-1]= (byte) 0;
			}
		}
		else if(type.contains("int")){
			t=TYPE_INT;
			val = Bytes.toBytes(SortedInt.
					toSortedInt(Integer.parseInt(value.substring(1, value.length()-1))));
		}
		else if(type.contains("long")){
			t=TYPE_LONG;
			val = Bytes.toBytes(SortedLong.
					toSortedLong(Long.parseLong(value.substring(1, value.length()-1))));
		}
		else if(type.contains("demical")){
			t=TYPE_DEMICAL;
			throw new NotSupportedDatatypeException(type);
		}
		else if(type.contains("float")){
			t=TYPE_FLOAT;
			val = Bytes.toBytes(Float.parseFloat(value.substring(1, value.length()-1)));
		}
		else if(type.contains("double")){
			t=TYPE_DOUBLE;
			val = Bytes.toBytes(Double.parseDouble(value.substring(1, value.length()-1)));
		}
		else if(type.contains("duration")){
			t=TYPE_DURATION;
			throw new NotSupportedDatatypeException(type);
		}
		else if(type.contains("datetime")){
			t=TYPE_DATETIME;
			throw new NotSupportedDatatypeException(type);
		}
		else if(type.contains("time")){
			t=TYPE_TIME;
			throw new NotSupportedDatatypeException(type);
		}
		else if(type.contains("date")){
			t=TYPE_DATE;
			System.out.println(type +" value: "+value);
			String date = value.substring(1, value.length()-1);
			StringTokenizer t2 = new StringTokenizer(date);
			int day = Integer.parseInt(t2.nextToken("/"));
			int month = Integer.parseInt(t2.nextToken("/"));
			int year = Integer.parseInt(t2.nextToken("/"));
			System.out.println(day+" "+month+" "+year);
			
			Calendar c = Calendar.getInstance();
			c.set(year, month, day);
			val = Bytes.toBytes(c.getTimeInMillis());
		}
		else {
			System.out.println(type +" value: "+value);
			throw new NotSupportedDatatypeException(type);
		}
		
		ret[0]=t;
		for (int i = 0; i < valueBytes; i++) {
			ret[i+1]=val[i];
		}
		
		if (ret.length!=totalBytes){
			System.exit(5);
		}
		
		return ret;
	}
}
