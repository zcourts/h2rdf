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
/*
 * (c) Copyright 2005, 2006, 2007 Hewlett-Packard Development Company, LP
 * All rights reserved.
 * [See end of file]
 */

package partialJoin;

import com.hp.hpl.jena.query.* ;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.MyOpVisitor;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;


/** Example 1 : Execute a simple SELECT query on a model
 *  to find the DC titles contained in a model. 
 * 
 * @author Andy Seaborne
 * @version $Id: Ex1.java,v 1.17 2007/01/02 11:46:47 andy_seaborne Exp $
 */

public class Ex2
{
    static public final String NL = System.getProperty("line.separator") ; 
    
    public static void main(String[] args)
    {
        String prolog = "PREFIX dc: <http://dbpedia.org/resource/>"+
        				"PREFIX p: <http://dbpedia.org/ontology/>"+
        				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
        				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
        				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>";
      
        

        String q1 = prolog + NL +
		"SELECT  ?x ?01" +
		"WHERE   { ?x rdf:type ub:Department ." +
		"?x ub:takesCourse <http://www.Department1.University0.edu/Course5>}";
        
        String q2 = prolog + NL +
		"SELECT  ?x ?y ?z ?012345" +
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
		"SELECT  ?x ?n ?em ?t ?0" +
		"WHERE   { ?x ub:worksFor <http://www.Department1.University0.edu> ." +
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
		"SELECT  ?x ?y ?2" +
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
		"SELECT  ?x ?z ?012345 " +
		"WHERE   { ?x rdf:type ub:Student ." +
		"?y rdf:type ub:Professor ." +
		"?z rdf:type ub:Course ." +
		"?x ub:advisor ?y ." +
		"?x ub:takesCourse ?z ." +
		"?y ub:teacherOf ?z}";
        
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
		"SELECT  ?x ?y ?03" +
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
        
        
        String inp = prolog + NL + args[1];
        System.out.println(inp);
        
        Query query = QueryFactory.create(inp) ;
        Op opQuery = Algebra.compile(query) ;
        System.out.println(opQuery) ;
        MyOpVisitor v = new MyOpVisitor("0", query);
        JoinPlaner.setTable(args[0], "4", "-1");
        JoinPlaner.setQuery(query);
        OpWalker.walk(opQuery, v);
        /*
         * old version
        Query query = QueryFactory.create(inp) ;
        Op opQuery = AlgebraCompiler.compile(query.getQueryElement(), null);
        System.out.println(opQuery.toString());
        MyOpVisitor v = new MyOpVisitor("11111", query);
        //v.setQuery(query);
        JoinPlaner.setQuery(query);
        OpWalker.walk(opQuery, v);*/
        
        
        
    }
}
