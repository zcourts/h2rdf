package gr.ntua.h2rdf.client;


import com.hp.hpl.jena.graph.Triple;

public class Store {
	private Executor executor;
	private Loader loader;
	private H2RDFConf conf;

	public Store(H2RDFConf conf) {
		this.conf=conf;
		executor = new Executor(conf.getAddress(), "/in", conf.getName());
		loader=new HbaseSequentialLoader(conf); //Default loader
	}

	public void add(Triple triple) {
		loader.add(triple);
	}

	public void close() {
		loader.close();
		executor.close();
		
	}

	public ResultSet exec(String q) {
		return executor.run(q);
	}

	public void setLoader(String type) {
		if(type.equals("BULK")){
			loader=new BulkLoader(conf);
		}
		else if(type.equals("HBASE_SEQUENTIAL")){
			loader=new HbaseSequentialLoader(conf);
		}
		else if(type.equals("HBASE_BULK")){
			loader=new HbaseBulkLoader(conf);
		}
	}

	public Loader getLoader() {
		return loader;
	}
}
