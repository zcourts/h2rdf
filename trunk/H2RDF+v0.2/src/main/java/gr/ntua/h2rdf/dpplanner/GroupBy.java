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

import gr.ntua.h2rdf.indexScans.ResultBGP;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class GroupBy implements DPJoinPlan{
	public DPJoinPlan subplan;
	private List<Var> groupVars;
	private OptimizeOpVisitorDPCaching visitor;
	private CachingExecutor cachingExecutor;

	public GroupBy(List<Var> groupVars, OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) {
		this.visitor =visitor;
		this.groupVars = groupVars;
		this.cachingExecutor = cachingExecutor;
	}

	@Override
	public int compareTo(DPJoinPlan o) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String print() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(OptimizeOpVisitorDPCaching visitor,
			CachingExecutor cachingExecutor) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Double getCost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double[] getStatistics(Integer joinVar) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ResultBGP> getResults() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void computeCost() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Integer> getOrdering() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setEdgeGraph(BitSet s) {
		// TODO Auto-generated method stub
		
	}
	
	
}
