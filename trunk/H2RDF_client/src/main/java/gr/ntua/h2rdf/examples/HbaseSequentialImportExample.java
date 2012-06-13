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
package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.client.*;

import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;


public class HbaseSequentialImportExample {

	public static void main(String[] args) {

		String address = "server1.org";
		String table = "DBname";
		String user = "user_name";
		H2RDFConf conf = new H2RDFConf(address, table, user);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);
		store.setLoader("HBASE_SEQUENTIAL");
		
		String d0_0="http://www.Department0.University0.edu/";
		String ub ="http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
		String rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		String arco = "http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.rdf#";
		try{
			for (int i = 0; i < 10; i++) {
				Node s = Node.createURI("<"+d0_0+"GraduateStudent"+i+">");
				Node p = Node.createURI("<"+rdf+"type"+">");
				Node o = Node.createURI("<"+arco+"WebObject"+">");
				Triple triple = Triple.create(s, p, o);
				store.add(triple);
				/*s = Node.createURI("<"+d0_0+"GraduateStudent"+i+">");
				p = Node.createURI("<"+rdf+"type"+">");
				o = Node.createURI("<"+ub+"GraduateStudent"+">");
				triple = Triple.create(s, p, o);
				store.add(triple);*/
			}
		} catch (NotSupportedDatatypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		//Query
		
		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.rdf#>";
		String NL = System.getProperty("line.separator") ;
		
		String q = prolog + NL +
				"SELECT  ?x " +
				"WHERE   { ?x rdf:type arco:WebObject . " +
				"}";
		
		QueryResult<BindingSet> rs=null;
		try {
			rs = store.execOpenRdf(q);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(2);
		}
		int count=0;
		try {
			while (rs.hasNext()) {
				BindingSet bindingSet = rs.next();
				String x=bindingSet.getValue("x").toString();
				//String score=bindingSet.getValue("score").toString();
				//String termLabel=bindingSet.getValue("termLabel").toString();
				System.out.println("x: "+x);//+" score: "+score+" termLabel: "+termLabel);
				count++;
			}
			rs.close();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(count);
		
		
		store.close();
	}

}
