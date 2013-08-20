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
package gr.ntua.h2rdf.bytes;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.AnonId;

public class ByteValues {
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
	
	public static Node translate(byte type, byte[] value, HTable table) throws NotSupportedDatatypeException {
		String ret = "";
        RDFDatatype datatype=null; 
        Node n = null;
        byte[] temp1,k;
        Get get;
		switch (type) {
		case TYPE_STRING:
			datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#string"); 
			temp1=new byte[ByteValues.totalBytes];
			temp1[0]=type;
			for (int i = 0; i < ByteValues.totalBytes-1; i++) {
				temp1[i+1]=value[i];
			}
			//byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			k = new byte[ByteValues.totalBytes+1];
			k[0]=(byte) 1;
			
			for (int j = 0; j < ByteValues.totalBytes; j++) {
				k[j+1]=temp1[j];
			}
			get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					ret+=Bytes.toString(result.raw()[0].getValue());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			n = Node.createLiteral(ret, "", datatype);
			break;
		case TYPE_BOOLEAN:
			datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#boolean"); 
			if(value[valueBytes-1]==(byte) 1)
				ret+=true;
			else
				ret+=false;
			n = Node.createLiteral(ret, "", datatype);
			break;
		case TYPE_INT:
			datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#int"); 
			ret+=SortedInt.toInt(Bytes.toLong(value));
			n = Node.createLiteral(ret, "", datatype);
			break;
		case TYPE_LONG:
			datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#long"); 
			ret+=SortedLong.toLong(Bytes.toLong(value));
			n = Node.createLiteral(ret, "", datatype);
			break;
		case TYPE_DEMICAL:
			throw new NotSupportedDatatypeException(type+"");
			
			//break;
		case TYPE_FLOAT:
			datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#float"); 
			ret+=Bytes.toFloat(value);
			n = Node.createLiteral(ret, "", datatype);
			break;
		case TYPE_DOUBLE:
			datatype = new BaseDatatype("http://www.w3.org/2001/XMLSchema#double"); 
			ret+=Bytes.toDouble(value);
			n = Node.createLiteral(ret, "", datatype);
			break;
		case TYPE_URI:
			temp1=new byte[ByteValues.totalBytes];
			temp1[0]=type;
			for (int i = 0; i < ByteValues.totalBytes-1; i++) {
				temp1[i+1]=value[i];
			}
			//byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			k = new byte[ByteValues.totalBytes+1];
			k[0]=(byte) 1;
			
			for (int j = 0; j < ByteValues.totalBytes; j++) {
				k[j+1]=temp1[j];
			}
			get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					ret+=Bytes.toString(result.raw()[0].getValue());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			n = Node.createURI(ret);
			break;

		case TYPE_NOTYPE:
			temp1=new byte[ByteValues.totalBytes];
			temp1[0]=type;
			for (int i = 0; i < ByteValues.totalBytes-1; i++) {
				temp1[i+1]=value[i];
			}
			//byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			k = new byte[ByteValues.totalBytes+1];
			k[0]=(byte) 1;
			
			for (int j = 0; j < ByteValues.totalBytes; j++) {
				k[j+1]=temp1[j];
			}
			get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					ret+=Bytes.toString(result.raw()[0].getValue());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			n = Node.createLiteral(ret);
			break;
			

		case TYPE_BLANK:
			temp1=new byte[ByteValues.totalBytes];
			temp1[0]=type;
			for (int i = 0; i < ByteValues.totalBytes-1; i++) {
				temp1[i+1]=value[i];
			}
			//byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			k = new byte[ByteValues.totalBytes+1];
			k[0]=(byte) 1;
			
			for (int j = 0; j < ByteValues.totalBytes; j++) {
				k[j+1]=temp1[j];
			}
			get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					ret+=Bytes.toString(result.raw()[0].getValue());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			n = Node.createAnon(AnonId.create(ret));
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
		
		return n;
	}
	
	public static String getStringValue(byte[] id) throws NotSupportedDatatypeException{
		String ret = "";
		byte[] l = new byte[valueBytes];
		for (int i = 0; i < l.length; i++) {
			l[i]=id[i+1];
		}
		switch (id[0]) {
		case TYPE_URI:
			ret+=id[0]+"|"+Bytes.toLong(l);
			break;
		case TYPE_STRING:
			ret+=id[0]+"|"+Bytes.toLong(l);
			break;
		case TYPE_BOOLEAN:
			ret+=id[0]+"|"+Bytes.toLong(l);
			
			break;
		case TYPE_NOTYPE:
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
		case TYPE_BLANK:
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

}
