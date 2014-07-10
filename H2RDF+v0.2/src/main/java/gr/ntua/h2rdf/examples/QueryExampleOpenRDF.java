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
package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.H2RDFFactory;
import gr.ntua.h2rdf.client.H2RDFQueryResult;
import gr.ntua.h2rdf.client.ResultSet;
import gr.ntua.h2rdf.client.Store;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;

import java.util.Iterator;
import java.util.List;

public class QueryExampleOpenRDF {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String address = "master";
		String table = args[0];
		String user = "root";
		H2RDFConf conf = new H2RDFConf(address, table, user);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);

		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
				"PREFIX yago: <http://www.w3.org/2000/01/rdf-schema#>"+
				"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>";
		String NL = System.getProperty("line.separator") ;
		
		String qq1 = prolog + NL +
				"SELECT  * " +
				"WHERE   { " +
				"?p <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?p1 ." +
				"?p <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?e1 ." +
				"?p1 <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?e2 ." +
				"}";
		
		String q1_0 = prolog + NL +
		"SELECT * " +
		"WHERE   { ?prof ub:teacherOf ?course1 . " +
		"?prof ub:teacherOf ?course2 . " +
		"?course1 rdf:type ub:Course . " +
		"?course2 rdf:type ub:GraduateCourse ." +
		"} " ;

		String q1 = prolog + NL +
				"SELECT  * " +
		        "WHERE   { ?x ub:advisor ?z ." +
		        "?x ub:takesCourse ?y ." +
		        "?z ub:teacherOf ?y } ";
		
        String q2_0 = prolog + NL +
        "SELECT  ?x ?z ?y " +
        "WHERE   { ?p rdf:type ?t2 . " +
			"?p ub:worksFor ?d ." +
	        "?p ub:teacherOf ?c . " +
	        "?s ub:takesCourse ?c .  } ";
        String q2_1 = prolog + NL +
		"SELECT  ?x ?y ?z " +
		"WHERE   { " +
		"?x rdf:type ub:GraduateStudent ." +
		"?z1 rdf:type ub:Department ." +
		"?x ub:memberOf ?z1 ." +
		"?z1 ub:subOrganizationOf ?y1 ." +
		"?x ub:undergraduateDegreeFrom ?y1 " +
		"}";
        
        String q2 = prolog + NL +
		"SELECT  ?x ?y ?z " +
		"WHERE   { " +
		"?x1 rdf:type ub:GraduateStudent ." +
		"?y rdf:type ub:University ." +
		"?z rdf:type ub:Department ." +
		"?x1 ub:memberOf ?z ." +
		"?z ub:subOrganizationOf ?y ." +
		"?x1 ub:undergraduateDegreeFrom ?y " +
		"}";
        
        String q3 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:Publication ." +
		"?x ub:publicationAuthor <http://www.Department1.University0.edu/AssistantProfessor5> }";
        
        String q4 = prolog + NL +
		"SELECT  ?x ?n ?em ?t" +
		"WHERE   { " +
		"?x ub:name ?n ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t ." +
		"?x ub:worksFor <http://www.Department0.University0.edu> ." +
		"?x rdf:type ub:FullProfessor ."+
		"}";
        
        String q5 = prolog + NL +
		"SELECT  ?x  " +
		"WHERE   {" +
		"?x rdf:type ub:UndergraduateStudent ." +
		"?x ub:memberOf <http://www.Department1.University0.edu> }";
        
        String q6 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:Student  }";
        
        String q7 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { " +
		"?y rdf:type ub:Course ." +
		"<http://www.Department0.University0.edu/FullProfessor0> ub:teacherOf ?y ." +
		"?x ub:takesCourse ?y " +
		"}";
        
        String q8 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String q9 = prolog + NL +
        		"SELECT  ?x ?z ?y " +
        		"WHERE   { ?x rdf:type ub:UndergraduateStudent ." +
        		"?z rdf:type ub:FullProfessor ." +
        		"?y rdf:type ub:Course ."+
        		"?x ub:advisor ?z ." +
        		"?x ub:takesCourse ?y ." +
        		"?z ub:teacherOf ?y }";
        
        String q10 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        String q11 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String q12 = prolog + NL +
		"SELECT  ?x ?y " +
		"WHERE   { ?x rdf:type ub:Professor ." +
		"?y rdf:type ub:Department ." +
		"?x ub:worksFor ?y ." +
		"?y ub:subOrganizationOf <http://www.University0.edu>}";
        
        String q13 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        String q14 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String q15 = prolog + NL +
        		"select (count(?a) as ?count) where {"+
        		"?a  rdf:type  arco:WebResource . "+
				"}";
        
        String q16 = prolog + NL +
        		"select ?s ?p?o where {" +
        		"?res arco:hasLanguage ?lang. " +
        		"?res rdf:type arco:WebResource. " +
        		"} group by ?lang ";
        
        String q=q9;
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
				//String termInstance=bindingSet.getValue("enrich").toString();
				//String score=bindingSet.getValue("o").toString();
				//String termLabel=bindingSet.getValue("termLabel").toString();
				/*String termInstance=bindingSet.getValue("eventLabel").toString();
				String score=bindingSet.getValue("entityType").toString();
				String termLabel=bindingSet.getValue("entityLabel").toString();*/
				System.out.println(bindingSet.toString());//+" ent: "+score);//+" termLabel: "+termLabel);
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
