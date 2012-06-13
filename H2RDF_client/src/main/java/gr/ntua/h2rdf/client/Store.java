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
package gr.ntua.h2rdf.client;


import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResult;

import gr.ntua.h2rdf.bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.graph.Triple;

public class Store {
	private Executor executor;
	private ExecutorOpenRdf executorOpenRdf;
	private Loader loader;
	private H2RDFConf conf;

	public Store(H2RDFConf conf) {
		this.conf=conf;
		executor = new Executor("/in", conf);
		executorOpenRdf = new ExecutorOpenRdf("/in", conf);
		loader=new HbaseSequentialLoader(conf); //Default loader
	}

	public void add(Triple triple) throws NotSupportedDatatypeException {
		loader.add(triple);
	}

	public void close() {
		loader.close();
		executor.close();
		
	}

	public ResultSet exec(String q) throws Exception {
		return executor.run(q);
	}


	public QueryResult<BindingSet> execOpenRdf(String q) throws Exception {
		return executorOpenRdf.run(q);
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
