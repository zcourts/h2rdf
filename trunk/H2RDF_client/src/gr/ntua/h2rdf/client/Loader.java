package gr.ntua.h2rdf.client;

import com.hp.hpl.jena.graph.Triple;

public interface Loader {
	
	public void add(Triple triple);
	public void close();
}
