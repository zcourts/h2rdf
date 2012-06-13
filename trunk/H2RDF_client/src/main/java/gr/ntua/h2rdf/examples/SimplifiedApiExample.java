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
package gr.ntua.h2rdf.examples;

import gr.ntua.h2rdf.client.SimplifiedAPI;

public class SimplifiedApiExample {

	public static void main(String[] args) {
		String triples = "thomas,name,moore-thomas,city,dublin-thomas,friend,nikos";
		SimplifiedAPI.putTriples("server1", "DBname", triples);
		SimplifiedAPI.runSPARQLQuery("server1", "DBname",
			"SELECT ?x WHERE {?x arcProp:city arc:dublin . ?x arcProp:friend arc:nikos}");
	
	}
}
