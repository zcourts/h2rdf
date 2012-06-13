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

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.H2RDFFactory;
import gr.ntua.h2rdf.client.H2RDFQueryResult;
import gr.ntua.h2rdf.client.ResultSet;
import gr.ntua.h2rdf.client.Store;
import org.openrdf.query.BindingSet;

import java.util.Iterator;
import java.util.List;

public class QueryExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String address = "server1.org";
		String table = "DBname";
		String user = "user_name";
		H2RDFConf conf = new H2RDFConf(address, table, user);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);

		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
				"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.rdf#>";
		String NL = System.getProperty("line.separator") ;
		
		String q = prolog + NL +
				"SELECT ?termInstance ?score ?termLabel " +
				"WHERE   { ?termInstance rdf:type arco:Term . " +
				"?termInstance arco:kyotoDomainRelevance ?score ." +
				"?termInstance rdfs:label ?termLabel " +
				"FILTER (?score >= \"70\"^^<http://www.w3.org/2001/XMLSchema#double>) "+
				" }";
		
		
		/*String q = prolog + NL +
				"SELECT  ?eventLabel ?time ?role ?entityInstance ?entityType ?entityLabel " +
				"WHERE   { ?event rdf:type arco:Event . " +
				"?event rdfs:label ?eventLabel . " +
				"?event arco:hasRole ?role . "+
				"?role arco:classifies ?entityInstance . "+
				"?entityInstance rdf:type ?entityType . "+
				"?entityInstance rdfs:label ?entityLabel . "+
				"?event arco:hasTime ?time"+
				" }";*/
		/*String q = prolog + NL +
				"SELECT  ?event ?eventLabel ?time ?entityType " +
				"?entityLabel ?realizationLabel ?start ?end ?doc " +
				"WHERE   { ?event rdf:type arco:Event . " +
				"?event rdfs:label ?eventLabel . " +
				"?event arco:hasTime ?time . "+
				"?event arco:hasRole ?role . "+
				"?role arco:classifies ?entityInstance . "+
				"?entityInstance rdf:type ?entityType . "+
				"?entityInstance rdfs:label ?entityLabel . "+
				"?event arco:isRealizedBy ?real . "+
				"?real rdfs:label ?realizationLabel . "+
				"?real arco:startOffset ?start . "+
				"?real arco:endOffset ?end . "+
				"?real arco:docLength ?doc . "+
				" }";*/

		/*String q = prolog + NL +
				"SELECT  ?x " +
				"WHERE   { ?x rdf:type ub:GraduateStudent . " +
				"FILTER (?x < 30.5)"+
				"?x ub:takesCourse <http://www.Department0.University0.edu/Course0> }";*/
		
		/*String q = prolog + NL +
				"SELECT  ?x ?y ?z "+
				"WHERE   { ?x rdf:type ub:GraduateStudent . "+
				" ?y rdf:type ub:University . "+
				" ?z rdf:type ub:Department . "+
				" ?x ub:memberOf ?z . "+
				" ?z ub:subOrganizationOf ?y . "+
				" ?x ub:undergraduateDegreeFrom ?y }";*/
		
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
			List<String> list =  r.getBindings("termInstance");
			List<String> list2 =  r.getBindings("score");
			List<String> list3 =  r.getBindings("termLabel");
			/*List<String> list =  r.getBindings("eventLabel");
			List<String> list2 =  r.getBindings("entityType");
			List<String> list3 =  r.getBindings("entityLabel");*/
			Iterator<String> it = list.iterator();
			while(it.hasNext()){
				String termInstance = it.next();
				Iterator<String> it2 = list2.iterator();
				while(it2.hasNext()){
					String score = it2.next();
					Iterator<String> it3 = list3.iterator();
					while(it3.hasNext()){
						System.out.println("termInstance: "+termInstance+" score: "+score+" termLabel: "+it3.next());
						count++;
					}
				}
			}
		}
		rs.close();
		System.out.println(count);
		
		store.close();
		
	}

}
