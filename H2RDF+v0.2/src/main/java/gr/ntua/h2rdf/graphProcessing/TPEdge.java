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
package gr.ntua.h2rdf.graphProcessing;

import org.jgrapht.graph.DefaultEdge;

public class TPEdge extends DefaultEdge implements Comparable<TPEdge>{

	int visited, processed;
	TriplePattern triplePattern;
	public Integer priority;
	
	public TPEdge(TriplePattern tp) {
		super();
		triplePattern=tp;
		visited =0;
		processed=0;
		priority = Integer.MAX_VALUE;
	}

	public void visit(){
		visited++;
	}

	public boolean isVisited(){
		return visited>0;
	}

	public boolean isProcessed(){
		return processed>0;
	}

	public void process(){
		processed++;
	}

	@Override
	public String toString() {
		return triplePattern + "";
	}

	public void reprocess() {
		processed=0;
		
	}

	@Override
	public int compareTo(TPEdge o) {
		
		return this.priority.compareTo(o.priority);
	}

}
