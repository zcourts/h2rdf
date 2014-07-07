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
				/*"SELECT  * " +
				"WHERE   { " +
				"?p <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?q ." +
				"?p <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?r ." +
				"?p <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?s ." +
				"?q <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?p ." +
				"?q <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?r ." +
				"?q <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?s ." +
				//"?q <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?w ." +
				//"?q <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?z ." +
				"?r <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?p ." +
				"?r <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?q ." +
				"?r <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?s ." +
				//"?r <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?w ." +
				//"?r <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?z ." +
				"?s <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?p ." +
				"?s <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?q ." +
				"?s <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?r ." +
				//"?s <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?w ." +
				//"?s <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?z ." +
				"?w <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?p ." +
				"?w <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?q ." +
				"?w <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?r ." +
				"?w <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?s ." +
				"?w <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?z ." +
				"?z <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?p ." +
				"?z <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?q ." +
				"?z <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?r ." +
				"?z <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?s ." +
				"?z <http://yago-knowledge.org/resource/hasInternalWikipediaLinkTo> ?w ." +
				"}";*/
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
		

        String qp = prolog + NL +
        		"select * where {" +
        		"?x1 <http://www.University0.edu> ?x2 . " +
        		"?x2 <http://www.University0.edu> ?x3 . " +
        		"?x3 <http://www.University0.edu> ?x4 . " +
        		"?x4 <http://www.University0.edu> ?x5 . " +
        		"?x5 <http://www.University0.edu> ?x6 . " +
        		"?x6 <http://www.University0.edu> ?x7 . " +
        		"?x7 <http://www.University0.edu> ?x8 . " +
        		"?x8 <http://www.University0.edu> ?x9 . " +
        		"?x9 <http://www.University0.edu> ?x10 . " +
        		"?x10 <http://www.University0.edu> ?x11 . " +
        		/*"?x11 rdf:type ?x12 . " +
        		"?x12 rdf:type ?x13 . " +
        		"?x13 rdf:type ?x14 . " +
        		"?x14 rdf:type ?x15 . " +
        		"?x15 rdf:type ?x16 . " +
        		"?x16 rdf:type ?x17 . " +
        		"?x17 rdf:type ?x18 . " +
        		"?x18 rdf:type ?x19 . " +
        		"?x19 rdf:type ?x20 . " +
        		"?x20 rdf:type ?x21 . " +*/
        		"} ";
        String qs = prolog + NL +
        		"select * where {" +
        		"?x <http://www.University1.edu> ?x1 . " +
        		"?x <http://www.University1.edu> ?x2 . " +
        		"?x <http://www.University1.edu> ?x3 . " +
        		"?x <http://www.University1.edu> ?x4 . " +
        		"?x <http://www.University1.edu> ?x5 . " +
        		"?x <http://www.University1.edu> ?x6 . " +
        		"?x <http://www.University1.edu> ?x7 . " +
        		"?x <http://www.University1.edu> ?x8 . " +
        		"?x <http://www.University1.edu> ?x9 . " +
        		"?x <http://www.University1.edu> ?x10 . " +
        		/*
        		"?x rdf:type ub:Professor . " +
        		"?x rdf:type ub:FullProfessor . " +
        		"?x rdf:type ub:AssistantProfessor . " +
        		"?x rdf:type ub:GraduateCourse . " +
        		"?x rdf:type ub:Department . " +
        		"?x rdf:type ub:Faculty . " +
        		"?x rdf:type ?x2 . " +
        		"?x ub:memberOf ?x3 . " +
        		"?x ub:subOrganizationOf ?x4 . " +
        		"?x ub:teacherOf ?x5 . " +
        		"?x ub:advisor ?x6 . " +
        		"?x ub:emailAddress ?x7 . " +
        		"?x ub:name ?x8 . " +
        		"?x ub:telephone ?x9 . " +*/
        		"} ";

        String qs1 = prolog + NL +
        		"select * where {" +
        		"?x rdf:type ?x2 . " +
        		"?x rdf:type ?x3 . " +
        		"?x rdf:type ?x4 . " +
        		"?x rdf:type ?x5 . " +
        		"?x rdf:type ?x6 . " +
        		"?x rdf:type ?x7 . " +
        		"?x rdf:type ?x8 . " +
        		"?x rdf:type ?x9 . " +
        		"?x rdf:type ?x10 . " +
        		"?x rdf:type ?x11 . " +
        		"?x rdf:type ?x12 . " +
        		"?x rdf:type ?x13 . " +
        		"?x rdf:type ?x14 . " +
        		"?x rdf:type ?x15 . " +
        		/*"?x rdf:type ?x16 . " +
        		"?x rdf:type ?x17 . " +
        		"?x rdf:type ?x18 . " +
        		"?x rdf:type ?x19 . " +
        		"?x rdf:type ?x20 . " +
        		"?x rdf:type ?x21 . " +*/
        		"} ";
        
        String qg = prolog + NL +
        		"select * where {" +
        		"?x1 <http://www.University1.edu> ?x2 . " +
        		"?x1 <http://www.University1.edu> ?x3 . " +
        		"?x2 <http://www.University1.edu> ?x4 . " +
        		"?x3 <http://www.University1.edu> ?x4 . " +
        		"?x2 <http://www.University1.edu> ?x5 . " +
        		"?x4 <http://www.University1.edu> ?x6 . " +
        		"?x5 <http://www.University1.edu> ?x6 . " +
        		"?x5 <http://www.University1.edu> ?x7 . " +
        		"?x6 <http://www.University1.edu> ?x8 . " +
        		"?x7 <http://www.University1.edu> ?x8 . " +
        		/*"?x7 rdf:type ?x8 . " +
        		"?x8 rdf:type ?x9 . " +
        		"?x5 rdf:type ?x10 . " +
        		"?x6 rdf:type ?x11 . " +
        		"?x9 rdf:type ?x12 . " +
        		"?x10 rdf:type ?x11 . " +
        		"?x11 rdf:type ?x12 . " +
        		"?x10 rdf:type ?x13 . " +
        		"?x13 rdf:type ?x14 . " +
        		"?x14 rdf:type ?x11 . " +*/
        		"} ";
        String qps = prolog + NL +
        		"select * where {" +
        		"?x rdf:type ?x2 . " +
        		"?x2 ub:memberOf ?x3 . " +
        		"?x rdf:type ub:Professor . " +
        		"?x4 ub:subOrganizationOf ?x5 . " +
        		"?x rdf:type ub:FullProfessor . " +
        		"?x6 ub:teacherOf ?x7 . " +
        		"?x rdf:type ub:AssistantProfessor . " +
        		"?x8 ub:advisor ?x9 . " +
        		"?x rdf:type ub:GraduateCourse . " +
        		"?x10 ub:emailAddress ?x11 . " +
        		"?x rdf:type ub:Department . " +
        		"?x12 ub:name ?x13 . " +
        		"?x rdf:type ub:Faculty . " +
        		"?x14 ub:telephone ?x15 . " +
        		"?x ub:headOf ?x16 . " +
        		"?x16 rdf:type ub:Student . " +
        		"?x rdf:type ub:Chair . " +
        		"?x18 rdf:type ub:Dean . " +
        		"?x rdf:type ub:GraduateStudent . " +
        		"?x20 rdf:type ub:Course . " +/**/
        		"} ";

        String qps1 = prolog + NL +
        		"select * where {" +
        		"?x <http://www.University1.edu> ?x2 . " +
        		"?x2 <http://www.University1.edu> <http://www.University1.edu> . " +
        		"?x <http://www.University1.edu> ?x4 . " +
        		"?x4 <http://www.University1.edu> <http://www.University1.edu> . " + 
        		"?x <http://www.University1.edu> ?x6 . " +
        		"?x6 <http://www.University1.edu> <http://www.University1.edu> . " +
        		"?x <http://www.University1.edu> ?x8 . " +
        		"?x8 <http://www.University1.edu> <http://www.University1.edu> . " +
        		"?x rdf:type ?x10 . " +
        		"?x10 <http://www.University1.edu> <http://www.University1.edu> . " +
        		/*"?x rdf:type ?x12 . " +
        		"?x12 ub:memberOf <http://www.University1.edu> . " +
        		"?x rdf:type ?x14 . " +
        		"?x14 ub:memberOf <http://www.University1.edu> . " +
        		"?x rdf:type ?x16 . " +
        		"?x16 ub:memberOf <http://www.University1.edu> . " +
        		"?x rdf:type ?x18 . " +
        		"?x18 ub:memberOf <http://www.University1.edu> . " +
        		"?x rdf:type ?x20 . " +
        		"?x20 ub:memberOf <http://www.University1.edu> . " +*/
        		"} ";

        String qc = prolog + NL +
        		"select * where {" +
        		"?x rdf:type ?x2 . " +
        		"?x2 rdf:type ?x3 . " +
        		"?x3 rdf:type ?x4 . " +
        		"?x4 rdf:type ?x5 . " +
        		"?x5 rdf:type ?x6 . " +
        		"?x6 rdf:type ?x7 . " +
        		"?x7 rdf:type ?x8 . " +
        		"?x8 rdf:type ?x9 . " +
        		"?x9 rdf:type ?x10 . " +
        		"?x10 rdf:type ?x11 . " +
        		"?x11 rdf:type ?x12 . " +
        		"?x12 rdf:type ?x13 . " +
        		"?x13 rdf:type ?x14 . " +
        		"?x14 rdf:type ?x15 . " +
        		"?x15 rdf:type ?x16 . " +
        		"?x16 rdf:type ?x17 . " +
        		"?x17 rdf:type ?x18 . " +
        		"?x18 rdf:type ?x19 . " +
        		"?x19 rdf:type ?x20 . " +
        		"?x20 rdf:type ?x . " +/**/
        		"} ";
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
