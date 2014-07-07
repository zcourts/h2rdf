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
import gr.ntua.h2rdf.indexScans.BGP;
import gr.ntua.h2rdf.loadTriples.ByteTriple;

import java.util.Arrays;
import java.util.BitSet;
import java.util.TreeMap;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class VarNode implements Comparable<VarNode>{
	public int similar;
	public Integer id;
	public String signature;
	private OptimizeOpVisitorDPCaching visitor;
	
	public VarNode(TreeMap<Integer, BitSet> varGraph,
			OptimizeOpVisitorDPCaching visitor, Integer i) {
		this.id=i;
		this.visitor=visitor;
		BitSet val = varGraph.get(i);
		String[] s = new String[val.cardinality()];
		int k1=0;
		for (int j = val.nextSetBit(0); j >= 0; j = val.nextSetBit(j+1)) {
			BGP bgp = visitor.bgpIds.get(j);
			s[k1] = getTripleSignatureNoVar(bgp, visitor.varIds.get(i));
			k1++;
		}
		Arrays.sort(s);
		signature = "";
		for (int j = 0; j < s.length; j++) {
			signature+=s[j];
		}
	}

	public int getSimilar() {
		return similar;
	}

	public void setSimilar(int similar) {
		this.similar = similar;
	}

	public String getSignature() {
		return signature;
	}

	public void setSignature(String signature) {
		this.signature = signature;
	}

	@Override
	public int compareTo(VarNode o) {
		if(this.similar < o.similar)
			return -41;
		else if(this.similar > o.similar)
			return 41;
		else{
			if(this.signature.compareTo(o.signature)==0)
				return new Integer(hashCode()).compareTo(o.hashCode());
			else
				return this.signature.compareTo(o.signature);
		}
	}
	
	public boolean equalsTo(VarNode o) {
			return this.similar==o.similar && this.signature==o.signature;
	}
	
	private String getTripleSignatureNoVar(BGP bgp, Var var) {
		String ret="";
		Triple t= bgp.bgp;
		ByteTriple btr = bgp.byteTriples.get(0);
		if(t.getSubject().equals(var)){
			ret+="$p";
			if(btr.getP()==0){
				ret+="?";
			}
			else{
				ret+=btr.getP();
			}
			ret+="$o";
			if(btr.getO()==0){
				ret+="?";
			}
			else{
				ret+=btr.getO();
			}
		}
		else if(t.getPredicate().equals(var)){
			ret+="$s";
			if(btr.getS()==0){
				ret+="?";
			}
			else{
				ret+=btr.getS();
			}
			ret+="$o";
			if(btr.getO()==0){
				ret+="?";
			}
			else{
				ret+=btr.getO();
			}
			
		}
		else if(t.getObject().equals(var)){
			ret+="$s";
			if(btr.getS()==0){
				ret+="?";
			}
			else{
				ret+=btr.getS();
			}
			ret+="$p";
			if(btr.getP()==0){
				ret+="?";
			}
			else{
				ret+=btr.getP();
			}
			
		}
		return ret;
	}
	
	@Override
	public String toString() {
		return "{"+visitor.varIds.get(id)+" sign: "+signature+"}";
	}
}
