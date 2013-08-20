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

import gr.ntua.h2rdf.loadTriples.ByteTriple;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;
import com.hp.hpl.jena.sparql.core.Var;


public class ResultBGP {
	public Triple bgp;
	public Set<Var> joinVars;
	public boolean isJoined;
	public long size;
	public Path path;
	private Map<Integer,double[]> stats;
	
	public String print(){
		String ret = "";
		for(Var v : joinVars)
			ret+=v+" ";
		return ret;
	}
	public ResultBGP(){
		
	}
	
	
	public ResultBGP(Set<Var> vars, Path path, Map<Integer,double[]> stats)  {
		isJoined = false;
		joinVars = vars;
		this.path = path;
		this.stats = stats;
	}
	/*
	 * ret[0] : ni join bindings for joinVar
	 * ret[1] : oi average bindings for each joinVar binding
	 */
	public double[] getStatistics (Var joinVar) throws IOException {
		return stats.get(OptimizeOpVisitorMergeJoin.varIds.get(joinVar).intValue());
	}
	
}
