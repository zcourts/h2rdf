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
package gr.ntua.h2rdf.dpplanner;

import gr.ntua.h2rdf.client.H2RDFConf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class TestCaching {
    static public final String NL = System.getProperty("line.separator") ; 
    
	public static void main(String[] args) {
		
		
		String prolog = "PREFIX dc: <http://dbpedia.org/resource/>"+
				"PREFIX p: <http://dbpedia.org/ontology/>"+
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"+
				"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>";



		String q1_0 = prolog + NL +
		"SELECT  * " +
		"WHERE   { ?x rdf:type ?z ." +
		"?x ub:takesCourse ?y ." +
		"}order by ?z ";
		
		String q1_1 = prolog + NL +
		"SELECT  * " +
		"WHERE   { ?x rdf:type ub:UndergraduateStudent ." +
		"?x ub:takesCourse ?y ." +
		"} order by ?y";
		
		String q1_3 = prolog + NL +
		"SELECT  * " +
		"WHERE   { " +
		"?x rdf:type ?t ." +
		"?x ub:takesCourse ?y ." +
		"}order by ?y";
		
		String q1_6 = prolog + NL +
		"SELECT  * " +
		"WHERE   { " +
		"?p rdf:type ?t ." +
		"?p ub:teacherOf ?y ." +
		"}order by ?y";
		
		
		String q1_5 = prolog + NL +
		"SELECT  * " +
		"WHERE { " +
		"?w rdf:type ?t ." +
		"?y1 rdf:type ub:GraduateCourse ." +
		"?w ub:teacherOf ?y1 ." +
		"}";
		
		String q1_4 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:UndergraduateStudent ." +
		"?x ub:takesCourse <http://www.Department0.University0.edu/Course1> ." +
		"}";
		
		
		
		String q1_2 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:UndergraduateStudent ." +
		"?x ub:takesCourse <http://www.Department0.University0.edu/Course1>}";
		
		String q2 = prolog + NL +
		"SELECT  ?x ?y ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?y rdf:type ub:University ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z ." +
		"?z ub:subOrganizationOf ?y ." +
		"?x ub:undergraduateDegreeFrom ?y " +
		"}";
		
		String q3 = prolog + NL +
		"SELECT  ?x ?1 " +
		"WHERE   { ?x rdf:type ub:Publication ." +
		"?x ub:publicationAuthor <http://www.Department1.University0.edu/AssistantProfessor5> }";
	
		
		String q4 = prolog + NL +
		"SELECT  ?x ?n " +
		"WHERE   { ?x ub:worksFor ?y ." +
		"?x rdf:type ?z ."+
		"?x ub:name ?n ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t " +
		"} " +
		"order by ?z ?y" ;

		String q4_0 = prolog + NL +
		"SELECT  ?x ?n " +
		"WHERE   { ?x ub:worksFor <http://www.Department0.University1.edu> ." +
		"?x rdf:type ub:FullProfessor ."+
		"?x ub:name ?n ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t " +
		"} " ;
		
		String q4_1 = prolog + NL +
		"SELECT  ?x ?n " +
		"WHERE   { ?x ub:worksFor <http://www.Department0.University1.edu> ." +
		"?x rdf:type ?y ."+
		"?x ub:name ?n ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t " +
		"} " ;
		
		String q4_2 = prolog + NL +
		"SELECT  ?x ?n " +
		"WHERE   { ?x ub:worksFor <http://www.Department0.University1.edu> ." +
		"?x rdf:type ub:FullProfessor ."+
		"?x ub:name \"FullProfessor1\" ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t " +
		"} " ;
		
		String q5 = prolog + NL +
		"SELECT  ?x  " +
		"WHERE   { ?x rdf:type ub:Student ." +
		"?x ub:memberOf <http://www.Department1.University0.edu> }";
		
		String q6 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:Student  }";

		String q7_0 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { "+
		"?w rdf:type ?z ." +
		"?w ub:teacherOf ?y ." +
		"}";
		String q7_3 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { "+
		"?w rdf:type ub:FullProfessor ." +
		"?y rdf:type ub:Course ." +
		"?w ub:teacherOf ?y ." +
		"}";
		String q7_1 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { " +
		"?y rdf:type ub:Course ." +
		"<http://www.Department0.University0.edu/FullProfessor0> ub:teacherOf ?y ." +
		"?x ub:takesCourse ?y " +
		"}";
		String q7_2 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { ?x rdf:type ub:UndergraduateStudent ." +
		"?y rdf:type ub:Course ." +
		"<http://www.Department1.University1.edu/FullProfessor3> ub:teacherOf ?y ." +
		"?x ub:takesCourse ?y " +
		"}";
		
		
		String q8 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";

		String q9_0 = prolog + NL +
		"SELECT  ?x ?z ?y " +
		"WHERE   {" +
		"?x1 ub:takesCourse ?y1 ." +
		"?x1 ub:advisor ?z1 ." +
		"?x1 rdf:type ub:GraduateStudent ." +
		"}";

		String q9_1 = prolog + NL +
		"SELECT  ?x ?z ?y " +
		"WHERE   { "+
		"?x2 rdf:type ub:GraduateStudent ." +
		"?x2 ub:advisor ?z2 ." +
		"?z2 ub:teacherOf ?y2  ." +
		"?x2 ub:takesCourse ?y2 ." +
		"?y2 rdf:type ub:GraduateCourse ."+
		"}";

		String q9_2 = prolog + NL +
		"SELECT  ?x ?z ?y " +
		"WHERE   { "+
		"?x3 ub:takesCourse ?y3 ." +
		"?x3 ub:advisor ?z3 ." +
		"?z3 ub:teacherOf ?y3 ." +
		"?x3 rdf:type ub:GraduateStudent ." +
		"?y3 rdf:type ub:GraduateCourse ."+
		"?z3 rdf:type ub:FullProfessor ." +
		"}";
		
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
		
		try {
        	String address = "clone17";
    		String t = "L100";
    		String user = "npapa";
    		H2RDFConf conf = new H2RDFConf(address, t, user);
    		Configuration hconf = conf.getConf();
    		hconf.set("hbase.rpc.timeout", "3600000");
    		hconf.set("zookeeper.session.timeout", "3600000");
			
	        CachingExecutor.connectTable(t, hconf);

	        CachingExecutor executor = new CachingExecutor(t,0);
			String inp = q4;
		    Query query = QueryFactory.create(inp) ;
	
		    executor.executeQuery(query,true, false);
	        
		    inp = q4_0;
		    query = QueryFactory.create(inp) ;
	
	        executor.executeQuery(query,true, false);
	        
	        inp = q4_1;
		    query = QueryFactory.create(inp) ;
	
	        executor.executeQuery(query,true, false);
	        inp = q4_2;
		    query = QueryFactory.create(inp) ;
	
	        executor.executeQuery(query,true, false);
	        /*inp = q7_2;
		    query = QueryFactory.create(inp) ;
	
	        executor.executeQuery(query);
	        
	        inp = q9_2;
		    query = QueryFactory.create(inp) ;
	
	        executor.executeQuery(query);*/
	        System.out.println(CacheController.resultCache.keySet());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
