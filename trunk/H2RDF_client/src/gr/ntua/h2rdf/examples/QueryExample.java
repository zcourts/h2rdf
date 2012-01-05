package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.H2RDFFactory;
import gr.ntua.h2rdf.client.Result;
import gr.ntua.h2rdf.client.ResultSet;
import gr.ntua.h2rdf.client.Store;

import java.util.Iterator;
import java.util.List;

public class QueryExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//String address = "ia200124.eu.archive.org";
		String address = "clone17";
		String name = "MyDatabase";
		H2RDFConf conf = new H2RDFConf(address, name);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);

		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>";
		String NL = System.getProperty("line.separator") ;
		
		String q = prolog + NL +
				"SELECT  ?x " +
				"WHERE   { ?x rdf:type ub:GraduateStudent . " +
				"FILTER (?x < 30.5)"+
				"?x ub:takesCourse <http://www.Department0.University0.edu/Course0> }";
		
		/*String q = prolog + NL +
				"SELECT  ?x ?y ?z "+
				"WHERE   { ?x rdf:type ub:GraduateStudent . "+
				" ?y rdf:type ub:University . "+
				" ?z rdf:type ub:Department . "+
				" ?x ub:memberOf ?z . "+
				" ?z ub:subOrganizationOf ?y . "+
				" ?x ub:undergraduateDegreeFrom ?y }";*/
		
		ResultSet rs=store.exec(q);
		Result r;
		int count=0;
		while((r=rs.getNext())!=null){
			List<String> list =  r.getBindings("x");
			Iterator<String> it = list.iterator();
			while(it.hasNext()){
				it.next();
				count++;
				//System.out.println(it.next());
			}
		}
		rs.close();
		System.out.println(count);
		
		store.close();
		
	}

}
