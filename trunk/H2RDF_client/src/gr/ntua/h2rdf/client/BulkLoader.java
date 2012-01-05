package gr.ntua.h2rdf.client;

import com.hp.hpl.jena.graph.Triple;

public class BulkLoader implements Loader {

	private int localChunkSize;
	
	public BulkLoader(H2RDFConf conf) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void add(Triple triple) {
		// TODO Auto-generated method stub
		
	}
	
	public void setLocalChunkSize(int localChunkSize) {
		this.localChunkSize=localChunkSize;
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void bulkUpdate() {
		// TODO Auto-generated method stub
		
	}

}
