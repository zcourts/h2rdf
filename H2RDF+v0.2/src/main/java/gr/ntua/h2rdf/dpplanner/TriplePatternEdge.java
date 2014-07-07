/*******************************************************************************
 * Copyright 2014 Nikolaos Papailiou
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.dpplanner;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.loadTriples.ByteTriple;


public class TriplePatternEdge implements Comparable<TriplePatternEdge>{
	public String srcPos,edgePos,destPos;
	public String edgeId,destId, signature;
	public List<VarNode> destVars;
	public Integer tripleId;
	private OptimizeOpVisitorDPCaching visitor;
	
	public TriplePatternEdge(OptimizeOpVisitorDPCaching visitor) {
		this.visitor=visitor;
		destVars=new ArrayList<VarNode>();
	}
	
	@Override
	public String toString() {
		return "{"+visitor.tripleIds.get(tripleId)+" sign: "+signature+"}";
	}

	@Override
	public int compareTo(TriplePatternEdge o) {
		return signature.compareTo(o.signature);
	}

	public void computeSignature() {
		signature="$"+srcPos+"0"+"$"+edgePos+edgeId+"$"+destPos+destId;
	}
}
