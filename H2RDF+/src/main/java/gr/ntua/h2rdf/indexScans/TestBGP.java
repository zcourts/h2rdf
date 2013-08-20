/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.indexScans;

import java.io.IOException;

import gr.ntua.h2rdf.client.H2RDFConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarAlloc;

public class TestBGP {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String d0_0="http://www.Department0.University0.edu/";
		String ub ="http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
		String rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		
		//Node s = Node.createURI("<http://www.Department0.University0.edu>");
		Node p = Node.createURI(ub+"advisor");
		//Node s = Node.createVariable("x");
		Var s = Var.alloc("x");

		//Node o = Node.createURI(ub+"Course");
		Var o = Var.alloc("y");
		//Node o = Node.createVariable("y");
		Triple tr = new Triple(s,p,o);
		String address = "clone17";
		String t = "LK1";
		String user = "npapa";
		H2RDFConf conf = new H2RDFConf(address, t, user);
		Configuration hconf = conf.getConf();
		HTable table=null, indexTable=null;
		try {
			table = new HTable( hconf, "LK1");
			indexTable = new HTable( hconf, "LK1_Index");
			BGP b = new BGP(tr, table, indexTable, null);
			b.processSubclass();
			
			
			double[] st = b.getStatistics((Var)tr.getSubject());
			System.out.println(st[0]+"\t"+st[1]);
			//b.printScan("?y");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
