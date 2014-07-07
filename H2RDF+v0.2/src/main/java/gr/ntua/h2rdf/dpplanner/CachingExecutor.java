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

import gr.ntua.h2rdf.indexScans.PartitionFinder;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HTable;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpWalker;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorMergeJoin;

public class CachingExecutor {

	public static HashMap<String, HTable> tables = new HashMap<String, HTable>();
	public static HashMap<String, HTable> indexTables = new HashMap<String, HTable>();
	public static HashMap<String, PartitionFinder> partitionFinders = new HashMap<String, PartitionFinder>();
	
	private static Configuration hconf ;
	private String table;
	public int id, tid;
	private HTable t, indexT;
	private PartitionFinder partitionFinder;
	public FileSystem fs;
	public OptimizeOpVisitorDPCaching visitor;
	
	public CachingExecutor(String table, int id) {
		this.table = table;
		this.id = id;
		tid=0;
		t = tables.get(table);
		indexT = indexTables.get(table);
		partitionFinder = partitionFinders.get(table);
	}
	
	public void executeQuery(Query query, boolean cacheRequests, boolean cacheResults) {
		Op opQuery = Algebra.compile(query);	
        System.out.println(opQuery); 
        visitor = new OptimizeOpVisitorDPCaching(query, this, t,indexT,partitionFinder,cacheRequests,cacheResults);
        OpWalker.walk(opQuery, visitor);

	}

	public static void connectTable(String table, Configuration conf) throws IOException {
		hconf = conf;
		if(!tables.containsKey(table)){
			HTable t =new HTable( hconf, table );
			HTable indexTable=null;
			if(table.equals("L20k")){
				indexTable = new HTable( hconf, "L20_Index" );
			}
			else{
				indexTable = new HTable( hconf, table+"_Index" );
			}
			tables.put(table, t);
			indexTables.put(table, indexTable);
			partitionFinders.put(table, new PartitionFinder(t.getStartEndKeys()));
			StatisticsCache.initialize(t);
		}
	}
	
	public static HTable getTable(String table) throws IOException{
		if(!tables.containsKey(table)){
			HTable t =new HTable( hconf, table );
			tables.put(table, t);
			return t;
		}
		else{
			return tables.get(table);
		}
	}

	public String getOutputFile() {
		return visitor.getOutputFile();
	}
	
	

}
