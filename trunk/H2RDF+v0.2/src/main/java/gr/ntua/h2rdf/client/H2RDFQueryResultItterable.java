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
package gr.ntua.h2rdf.client;

import gr.ntua.h2rdf.bytes.ByteValues;
import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.openrdf.query.Binding;
import org.openrdf.query.impl.MapBindingSet;

import com.hp.hpl.jena.graph.Node;


public class H2RDFQueryResultItterable {
	private List<MapBindingSet> bindlist, temp;
	private Iterator<MapBindingSet> it;

	private HashMap<String,List<String>> results;
	
	public H2RDFQueryResultItterable(String line, HTable table) {
		bindlist = new LinkedList<MapBindingSet>();
		results = new HashMap<String, List<String>>();
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		System.out.println(line);
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
				List<String> l = results.get(var);
				if(l==null)
					l = new ArrayList<String>();
				l.addAll(list);
				results.put(var, l);
			}
		}
		
		Iterator<String> kit = results.keySet().iterator();
		if(kit.hasNext()){
			String kvar =kit.next();
			Iterator<String> vals = results.get(kvar).iterator();
			bindlist= new LinkedList<MapBindingSet>();
			while(vals.hasNext()){
				String val = vals.next();
				MapBindingSet prevBind = new MapBindingSet();
				prevBind.addBinding(kvar, new MyValue(val));
				bindlist.add(prevBind);
			}
		}

		while(kit.hasNext()){
			String kvar =kit.next();
			temp=bindlist;
			Iterator<MapBindingSet> previt = temp.iterator();
			bindlist= new LinkedList<MapBindingSet>();
			while(previt.hasNext()){
				MapBindingSet prevBind = previt.next();
				if(prevBind==null){
					prevBind = new MapBindingSet();
				}
				Iterator<String> vals = results.get(kvar).iterator();
				while(vals.hasNext()){
					MapBindingSet b = new MapBindingSet();
					Iterator<Binding> w = prevBind.iterator();
					while(w.hasNext()){
						b.addBinding(w.next());
					}
					String val = vals.next();
					b.addBinding(kvar, new MyValue(val));
					bindlist.add(b);
					
				}
			}
		}
		it = bindlist.iterator();
	}

	private String translate(String temp, HTable table) {
		System.out.println("translate: "+temp);
		byte[] temp3=Bytes.toBytes(Long.parseLong(temp.substring(temp.indexOf("|")+1)));
		String ret="";
		try {
			Node n =ByteValues.translate((byte) new Byte(temp.substring(0,temp.indexOf("|"))), temp3, table);
			ret = n.toString(false);
			/*if(n.isURI()){
				ret = "<"+n.toString()+">";
			}
			else{
				ret = n.toString();
			}*/
			System.out.println(ret);
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

	public boolean hasNext() {
		return it.hasNext();
	}

	public MapBindingSet next() {
		return it.next();
	}
}
