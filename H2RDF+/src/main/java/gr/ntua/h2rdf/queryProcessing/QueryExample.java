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
package gr.ntua.h2rdf.queryProcessing;

import gr.ntua.h2rdf.client.H2RDFConf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;

public class QueryExample {
    static public final String NL = System.getProperty("line.separator") ; 

	/**
	 * @param args
	 */
	public static void main(String[] args) {

        String prolog = "PREFIX dc: <http://dbpedia.org/resource/>"+
        				"PREFIX p: <http://dbpedia.org/ontology/>"+
        				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
        				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
        				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"+
						"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>";
      
        

        String q1 = prolog + NL +
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
		"SELECT  ?x ?n ?em ?t" +
		"WHERE   { ?x ub:worksFor <http://www.Department0.University0.edu> ." +
		"?x rdf:type ub:Professor ."+
		"?x ub:name ?n ." +
		"?x ub:emailAddress ?em ." +
		"?x ub:telephone ?t " +
		"}";
        
        String q5 = prolog + NL +
		"SELECT  ?x  " +
		"WHERE   { ?x rdf:type ub:Student ." +
		"?x ub:memberOf <http://www.Department1.University0.edu> }";
        
        String q6 = prolog + NL +
		"SELECT  ?x " +
		"WHERE   { ?x rdf:type ub:Student  }";
        
        String q7 = prolog + NL +
		"SELECT  ?x ?y" +
		"WHERE   { ?x rdf:type ub:Student ." +
		"?y rdf:type ub:Course ." +
		"<http://www.Department1.University0.edu/FullProfessor3> ub:teacherOf ?y ." +
		"?x ub:takesCourse ?y " +
		"}";
        
        String q8 = prolog + NL +
		"SELECT  ?x ?z " +
		"WHERE   { ?x rdf:type ub:GraduateStudent ." +
		"?z rdf:type ub:Department ." +
		"?x ub:memberOf ?z}";
        
        String q9 = prolog + NL +
		"SELECT  ?x ?z ?y " +
		"WHERE   { ?x rdf:type ub:Student ." +
		"?z rdf:type ub:Professor ." +
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
        
        try {
        	String address = "master";
    		String t = "L3";
    		String user = "username";
    		H2RDFConf conf = new H2RDFConf(address, t, user);
    		Configuration hconf = conf.getConf();
    		hconf.set("hbase.rpc.timeout", "3600000");
    		hconf.set("zookeeper.session.timeout", "3600000");
			QueryPlanner.connectTable(t, hconf);
			String inp = q9;// args[1];
	        
	        Query query = QueryFactory.create(inp) ;

	        // Generate algebra
	        Op opQuery = Algebra.compile(query) ;
	        QueryPlanner planner = new QueryPlanner(query, t, "0");
	        planner.executeQuery();
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        

	}

}
