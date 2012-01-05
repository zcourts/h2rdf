package gr.ntua.h2rdf.client;


public class H2RDFFactory {

	
	public H2RDFFactory() {
		
	}
	
	public Store connectStore(H2RDFConf conf) {
		Store store = new Store(conf);
		return store;
	}

	public Store newStore(H2RDFConf conf) {
		Store store = new Store(conf);
		return store;
	}

}
