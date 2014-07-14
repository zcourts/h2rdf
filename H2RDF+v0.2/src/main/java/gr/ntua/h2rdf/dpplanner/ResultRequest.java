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

import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

public class ResultRequest implements Comparable<ResultRequest>{
	public List<Integer> ordering;
	public BitSet queries;
	public Double benefit, cost, benefitPerCost;
	public String label;
	private OptimizeOpVisitorDPCaching visitor;
	
	public ResultRequest(List<Integer> ordering, double benefit, double cost,String label) {
		queries = new BitSet();
		this.ordering=ordering;
		this.benefit=benefit;
		this.cost=cost;
		this.benefitPerCost=benefit/cost;
		this.label = label;
	}
	
	@Override
	public String toString() {
		benefitPerCost=benefit/cost;
		String ret = "{ label: "+label +" order: "+ordering+" benefit: "+benefit+"}";//+" cost: "+cost+"}";
		return ret;
	}

	@Override
	public int compareTo(ResultRequest o) {
		//benefitPerCost=benefit/cost;
		//double otherbenefitPerCost = o.benefit/o.cost;
		return o.benefit.compareTo(benefit);
	}

	public int compareToNatural(ResultRequest o) {
		//benefitPerCost=benefit/cost;
		//double otherbenefitPerCost = o.benefit/o.cost;
		return benefit.compareTo(o.benefit);
	}

	public void computeBenefitPerCost() {
		benefitPerCost=benefit/cost;
	}

	public String getSPARQLQuery() throws IOException {
		String ret = "SELECT * WHERE { \n";
		StringTokenizer tok = new StringTokenizer(label);
		HTable indexTable = visitor.indexTable;
		while(tok.hasMoreTokens()){
			String tq = tok.nextToken("&");
			String[] tq1 = tq.split("_");
			for (int i = 0; i < tq1.length; i++) {
				if(tq1[i].startsWith("$")){
					ret+="?v"+tq1[i].substring(1)+" ";
				}
				else{
					SortedBytesVLongWritable v = new SortedBytesVLongWritable(Long.parseLong(tq1[i]));
					
					Get get = new Get(v.getBytesWithPrefix());
					get.addColumn(Bytes.toBytes("2"), new byte[0]);
					Result res = indexTable.get(get);
					if(res.isEmpty())
						throw new IOException("node not found");
					ret+=Bytes.toString(res.value())+" ";
				}
				if(i==2){
					ret+=". \n";
				}
			}
		}
		ret+="} ";
		if(ordering.size()>0){
			ret+= "order by ";
			for(Integer i : ordering){
				ret+="?v"+i+" ";
			}
		}
		return ret;
	}

	public void setVisitor(OptimizeOpVisitorDPCaching visitor) {
		this.visitor=visitor;
	}
}
