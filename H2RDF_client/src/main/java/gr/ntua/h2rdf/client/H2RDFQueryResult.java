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
package gr.ntua.h2rdf.client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import gr.ntua.h2rdf.bytes.ByteValues;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;


public class H2RDFQueryResult {
	
	private HashMap<String,List<String>> results;

	public H2RDFQueryResult(String line, HTable table) {
		//System.out.println(line);
		results = new HashMap<String, List<String>>();
		StringTokenizer tokenizer = new StringTokenizer(line);
		//System.out.println(line);
		StringTokenizer tok = null;
		tokenizer.nextToken("!");
		while (tokenizer.hasMoreTokens()) {
			String b=tokenizer.nextToken("!");
			//System.out.println(b+" dfgdfs");
			if(b.startsWith("?")){
				tok=new StringTokenizer(b);
				String var=tok.nextToken("#");
				var=var.substring(1);
				//System.out.println(var);
				String bindings = tok.nextToken("#");
				//System.out.println(var+" "+bindings);
				List<String> list =new LinkedList<String>();
				StringTokenizer tok2 = new StringTokenizer(bindings);
				while (tok2.hasMoreTokens()) {
					String bind=tok2.nextToken("_");
					if(!bind.startsWith("!")){
						list.add(translate(bind, table));
					}
					
				}
				//System.out.println(var+" "+bindings);
				results.put(var, list);
			}
		}
	}
	
	private String translate(String temp, HTable table) {
		byte[] temp3=Bytes.toBytes(Long.parseLong(temp.substring(temp.indexOf("|")+1)));
		String ret="";
		try {
			ret = ByteValues.translate((byte) new Byte(temp.substring(0,temp.indexOf("|"))), temp3, table);
			return ret;
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ret;
		} catch (NotSupportedDatatypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ret;
		}
		/*if(temp.startsWith("1")){
			byte[] temp1=new byte[ByteValues.totalBytes];
			String ret="";
			temp1[0]=(byte) new Byte(temp.substring(0,temp.indexOf("|")));
			for (int i = 0; i < ByteValues.totalBytes-1; i++) {
				temp1[i+1]=temp3[i];
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
		}
		else{
		}*/
	}

	@Override
	public String toString() {
		return null;
		//return line;
	}

	public List<String> getBindings(String var) {
		return results.get(var);
	}

	public void itterate() {
		// TODO Auto-generated method stub
		
	}

	
}
