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
