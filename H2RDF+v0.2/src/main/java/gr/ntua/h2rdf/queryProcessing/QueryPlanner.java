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
package gr.ntua.h2rdf.queryProcessing;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;

public class QueryPlanner {
	public static HashMap<String, HTable> tables = new HashMap<String, HTable>();
	public static HashMap<String, HTable> indexTables = new HashMap<String, HTable>();
	private static Configuration hconf ;
	private String table, id;
	private Query query;
	private HTable t, indexT;
	private FileSystem fs;
	
	public static void connectTable(String table, Configuration conf) throws IOException{
		hconf = conf;
		if(!tables.containsKey(table)){
			HTable t =new HTable( hconf, table );
			HTable indexTable = new HTable( hconf, table+"_Index" );
			tables.put(table, t);
			indexTables.put(table, indexTable);
		}
	}
	
	public static void connectTableOnly(String table, Configuration conf) throws IOException{
		hconf = conf;
		if(!tables.containsKey(table)){
			HTable t =new HTable( hconf, table );
			//HTable indexTable = new HTable( hconf, table+"_Index" );
			tables.put(table, t);
			indexTables.put(table, null);
		}
	}
	public QueryPlanner(Query query, String table, String id) {
		this.table = table;
		this.id =id;
		this.query = query;
		t = tables.get(table);
		indexT = indexTables.get(table);
	}

	public void executeQuery() {
		try {
			fs= FileSystem.get(hconf);

	        Op opQuery = Algebra.compile(query);	        //op = Algebra.optimize(op) ;
	        //System.out.println(op) ;
	        

	        //OptimizeOpVisitor v = new OptimizeOpVisitor(query, t, indexT);
	        OptimizeOpVisitorMergeJoin v = new OptimizeOpVisitorMergeJoin(query, t, indexT);
	        OpWalker.walk(opQuery, v);
	        
			/*List<String> vars = query.getResultVars();
			Iterator<String> it = vars.iterator();
			while(it.hasNext()){
				System.out.println(it.next());
			}*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	
	
	
}
