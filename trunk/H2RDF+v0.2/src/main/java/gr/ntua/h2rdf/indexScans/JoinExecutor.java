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
package gr.ntua.h2rdf.indexScans;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.core.Var;

public class JoinExecutor {
	public static int joinId=0;
	
	public static List<ResultBGP> execute(Map<Var, JoinPlan> m, HTable table, HTable indexTable, boolean centalized) throws Exception {
		
		System.out.println(m);
		List<ResultBGP> ret= null;
		if(centalized){
			ret = CentralizedJoinExecutor.execute(m,  table,  indexTable);
		}
		else{
			ret = MapReduceJoinExecutor.execute( m,  table,  indexTable);
		}
		joinId++;
		
		return ret;
	}

	public static List<ResultBGP> executeMerge(MergeJoinPlan plan,
			Var joinVar, HTable table, HTable indexTable, boolean centalized) throws Exception {
		System.out.println(plan);
		List<ResultBGP> ret= null;
		if(centalized){
			ret = CentralizedMergeJoinExecutor.execute(plan,joinVar,  table,  indexTable,null);
		}
		else{
			ret = MapReduceMergeJoinExecutor1.execute( plan,joinVar,  table,  indexTable, null);
		}
		joinId++;
		return ret;
	}

	public static List<ResultBGP> executeMerge1(MergeJoinPlan plan,
			Var joinVar, HTable table, HTable indexTable, boolean centalized,
			OptimizeOpVisitorDPCaching visitor) throws Exception{
		List<ResultBGP> ret= null;
		if(centalized){
			ret = CentralizedMergeJoinExecutor.execute(plan,joinVar,  table,  indexTable,visitor);
		}
		else{
			ret = MapReduceMergeJoinExecutor1.execute( plan,joinVar,  table,  indexTable, visitor);
		}
		visitor.cachingExecutor.tid++;
		
		return ret;
	}

	public static List<ResultBGP> executeOrdering(ResultBGP resultBGP,
			long[][] maxPartition, List<Integer> orderVarsInt, boolean centralized, String table, OptimizeOpVisitorDPCaching visitor) throws Exception {
		List<ResultBGP> ret= null;
		if(centralized){
			ret = CentralizedOrderingExecutor.execute(resultBGP, maxPartition, orderVarsInt, table, visitor);
		}
		else{
			ret = MROrderingExecutor.execute(resultBGP, maxPartition, orderVarsInt, table, visitor);
		}
		visitor.cachingExecutor.tid++;
		return ret;
	}
	
	

}
