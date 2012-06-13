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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import gr.ntua.h2rdf.client.BulkLoader;
import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.H2RDFFactory;
import gr.ntua.h2rdf.client.Store;

import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

public class SimplifiedAPI {
	
	static final Integer CHUNK_SIZE = 5000;

	static String arc ="http://www.arcomem.com/";
	static String arcProp = "http://www.arcomem.com/property#";
	
	
	
	public static void main(String[] args) {
		// java -jar adress name triplestring
		String address = args[0];
		String name = args[1];
		String triplestring = args[2];
		putTriples(address, name, triplestring);
	}
	
	public static void putTriples(String address, String name, String triplestring) {
		
		//StringParsing
		ArrayList<String[]> triples = new ArrayList<String[]>();
		String[] intermediaryTriples = triplestring.split("-");
		for (int i = 0; i < intermediaryTriples.length; i++) {
			String[] tempArray = intermediaryTriples[i].split(",");
			triples.add(tempArray);
		}
		
		H2RDFConf conf = new H2RDFConf(address, name, "user_name");
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);
		store.setLoader("HBASE_BULK");
	
		HbaseBulkLoader bulkLoader = (HbaseBulkLoader) store.getLoader();
		bulkLoader.setChunkSize(CHUNK_SIZE);
		try{
			for (int i = 0; i < triples.size(); i++) {
				String[] tripleParams = triples.get(i);
				Node s = Node.createURI("<"+arc+tripleParams[0]+">");
				Node p = Node.createURI("<"+arcProp+tripleParams[1]+">");
				Node o = Node.createURI("<"+arc+tripleParams[2]+">");
		
				Triple triple = Triple.create(s, p, o);
				
				store.add(triple);	
			}
		} catch (NotSupportedDatatypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		store.close();
	}

	public static void runSPARQLQuery(String address, String name, String query) {
		
		H2RDFConf conf = new H2RDFConf(address, name, "user_name");
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);

		String prolog = "PREFIX arc: <" + arc +"> "+
				"PREFIX arcProp: <" + arcProp + ">";
		String NL = System.getProperty("line.separator") ;
		
		String q = prolog + NL + query;
		
		ResultSet rs=null;
		try {
			rs = store.exec(q);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(2);
		}
		H2RDFQueryResult r;
		int count=0;
		while((r=rs.getNext())!=null){
			List<String> list =  r.getBindings("x");
			Iterator<String> it = list.iterator();
			while(it.hasNext()){
				//it.next();
				//count++;
				System.out.println(it.next());
			}
		}
		rs.close();
		System.out.println(count);
		
		store.close();
		
	}
}
