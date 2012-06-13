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
