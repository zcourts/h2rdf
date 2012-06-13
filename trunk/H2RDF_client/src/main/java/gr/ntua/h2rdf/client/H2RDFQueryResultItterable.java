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
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResult;
import org.openrdf.query.impl.MapBindingSet;

public class H2RDFQueryResultItterable {
	private List<MapBindingSet> bindlist, temp;
	private Iterator<MapBindingSet> it;

	private HashMap<String,List<String>> results;
	
	public H2RDFQueryResultItterable(String line, HTable table) {
		bindlist = new LinkedList<MapBindingSet>();
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

	public boolean hasNext() {
		return it.hasNext();
	}

	public MapBindingSet next() {
		return it.next();
	}
}
