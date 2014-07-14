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
import java.util.List;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;


public interface DPJoinPlan extends Comparable<DPJoinPlan>{
	
	public String print();
	public void execute(OptimizeOpVisitorDPCaching visitor, CachingExecutor cachingExecutor) throws Exception;
	public Double getCost();
	public double[] getStatistics (Integer joinVar) throws IOException;
	public List<ResultBGP> getResults () throws IOException;
	public void computeCost() throws IOException;
	public List<Integer> getOrdering();
}
