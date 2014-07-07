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
package gr.ntua.h2rdf.bytes;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;


import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;

public class H2RDFNode {
	byte[] hashValue, stringValue;
	Node node;

	public static final int totalBytes = 9;
	public static final int typeBytes = 1;
	public static final int valueBytes = 8;

	public static final byte  TYPE_URI= (byte)0;
	public static final byte  TYPE_STRING= (byte)1;
	public static final byte  TYPE_BOOLEAN=(byte)2;
	public static final byte  TYPE_INT=(byte)3;
	public static final byte  TYPE_LONG=(byte)4;
	public static final byte  TYPE_NOTYPE=(byte)5;
	public static final byte  TYPE_FLOAT=(byte)6;
	public static final byte  TYPE_DOUBLE=(byte)7;
	public static final byte  TYPE_DURATION=(byte)8;
	public static final byte  TYPE_DATETIME=(byte)9;
	public static final byte  TYPE_TIME=(byte)10;
	public static final byte  TYPE_DATE=(byte)11;
	public static final byte  TYPE_BLANK= (byte)12;
	public static final byte  TYPE_DEMICAL=(byte)13;
	
	public H2RDFNode(Node node) throws NotSupportedDatatypeException {
		this.node=node;
		hashValue = new byte[totalBytes];
		stringValue = null;
		byte[] val = null;
		byte t = TYPE_NOTYPE;//default
		if(node.isBlank()){
			t=TYPE_BLANK;
			String value = node.toString();
			MD5Hash md5h = MD5Hash.digest(value);
			long hashVal = md5h.halfDigest();
			val = Bytes.toBytes(hashVal);
			stringValue = Bytes.toBytes(value);
		}
		else if(node.isLiteral()){
			String id =node.toString();
			if(id.contains("\"^^")){//type
				//System.out.println(id);
				StringTokenizer tokenizer = new StringTokenizer(id);
				String value = tokenizer.nextToken("^^");
				value = value.substring(1, value.length()-1);
				String type = tokenizer.nextToken();
				type = type.substring(type.lastIndexOf("#")+1, type.length());
				//System.out.println(type +" value: "+value);
				if(type.contains("string")){
					t=TYPE_STRING;
					//System.out.println(t +" value: "+value);
					MD5Hash md5h = MD5Hash.digest(value);
					long hashVal = md5h.halfDigest();
					val = Bytes.toBytes(hashVal);
					stringValue = Bytes.toBytes(value);
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
							toSortedInt(Integer.parseInt(value)));
				}
				else if(type.contains("long")){
					t=TYPE_LONG;
					val = Bytes.toBytes(SortedLong.
							toSortedLong(Long.parseLong(value)));
				}
				else if(type.contains("demical")){
					t=TYPE_DEMICAL;
					throw new NotSupportedDatatypeException(type);
				}
				else if(type.contains("float")){
					t=TYPE_FLOAT;
					val = Bytes.toBytes(Float.parseFloat(value));
				}
				else if(type.contains("double")){
					t=TYPE_DOUBLE;
					val = Bytes.toBytes(Double.parseDouble(value));
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
					//System.out.println(type +" value: "+value);
					StringTokenizer t2 = new StringTokenizer(value);
					int day = Integer.parseInt(t2.nextToken("/"));
					int month = Integer.parseInt(t2.nextToken("/"));
					int year = Integer.parseInt(t2.nextToken("/"));
					//System.out.println(day+" "+month+" "+year);
					
					Calendar c = Calendar.getInstance();
					c.set(year, month, day);
					val = Bytes.toBytes(c.getTimeInMillis());
					stringValue = Bytes.toBytes(value);
				}
				else {
					System.out.println(type +" value: "+value);
					throw new NotSupportedDatatypeException(type);
				}
			}
			else{
				t = TYPE_NOTYPE;
				//System.out.println(t +" value: "+id);
				id = id.substring(1, id.length()-1);
				MD5Hash md5h = MD5Hash.digest(id);
				long hashVal = md5h.halfDigest();
				val = Bytes.toBytes(hashVal);
				stringValue = Bytes.toBytes(id);
			}
		}
		else if(node.isURI()){
			t=TYPE_URI;
			String value = node.toString();
			MD5Hash md5h = MD5Hash.digest(value);
			long hashVal = md5h.halfDigest();
			val = Bytes.toBytes(hashVal);
			stringValue = Bytes.toBytes(value);
		}
		else{
			throw new NotSupportedDatatypeException("");
		}

		hashValue[0]=t;
		for (int i = 0; i < valueBytes; i++) {
			hashValue[i+1]=val[i];
		}	
	}

	public byte[] getHashValue() {
		return hashValue;
	}

	public byte[] getStringValue() {
		return stringValue;
	}
	
	public String getString(){
		if(node.isBlank()){
			return node.toString();
		}
		else if(node.isLiteral()){
			return node.toString();
		}
		else if(node.isURI()){

			return "<"+node.toString()+">";
		}
		else{
			return node.toString();
			
		}
	}
	
}
