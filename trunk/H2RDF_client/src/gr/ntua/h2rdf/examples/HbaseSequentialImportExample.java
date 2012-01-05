package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.client.*;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;


public class HbaseSequentialImportExample {

	public static void main(String[] args) {
		
		String address = "ia200124";
		String name = "MyDatabase";
		H2RDFConf conf = new H2RDFConf(address, name);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);
		store.setLoader("HBASE_SEQUENTIAL");
		
		String d0_0="http://www.Department0.University0.edu/";
		String ub ="http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
		String rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		
		for (int i = 0; i < 10000; i++) {
			Node s = Node.createURI("<"+d0_0+"GraduateStudent"+i+">");
			Node p = Node.createURI("<"+ub+"takesCourse"+">");
			Node o = Node.createURI("<"+d0_0+"Course0"+">");
			Triple triple = Triple.create(s, p, o);
			store.add(triple);
			s = Node.createURI("<"+d0_0+"GraduateStudent"+i+">");
			p = Node.createURI("<"+rdf+"type"+">");
			o = Node.createURI("<"+ub+"GraduateStudent"+">");
			triple = Triple.create(s, p, o);
			store.add(triple);
		}
		
		
		
		/*
		//Query
		
		String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>";
		String NL = System.getProperty("line.separator") ;
		
		String q = prolog + NL +
				"SELECT  ?x " +
				"WHERE   { ?x rdf:type ub:GraduateStudent . " +
				"?x ub:takesCourse <http://www.Department0.University0.edu/Course0> }";
		
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
		*/
		
		store.close();
	}

}
