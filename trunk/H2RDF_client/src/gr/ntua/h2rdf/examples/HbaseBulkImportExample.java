package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.client.*;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;


public class HbaseBulkImportExample {

	public static void main(String[] args) {
		
		String address = "ia200124";
		String name = "MyDatabase";
		H2RDFConf conf = new H2RDFConf(address, name);
		H2RDFFactory h2fact = new H2RDFFactory();
		Store store = h2fact.connectStore(conf);
		store.setLoader("HBASE_BULK");
		
		String d0_0="http://www.Department0.University0.edu/";
		String ub ="http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
		String rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		
		HbaseBulkLoader bulkLoader = (HbaseBulkLoader) store.getLoader();
		bulkLoader.setChunkSize(1000); // put data to HBase after 1000 triples
		
		for (int i = 0; i < 50000; i++) {
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

		store.close();
	}

}
