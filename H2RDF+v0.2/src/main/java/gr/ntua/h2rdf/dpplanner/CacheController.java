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

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.concurrent.QueueDP;
import gr.ntua.h2rdf.concurrent.SyncPrimitive;
import gr.ntua.h2rdf.dpplanner.CachedResults;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching;

public class CacheController extends Thread{
	public static HashMap<String,CachedResults> resultCache=new HashMap<String, CachedResults>();
	public static HashMap<String,List<ResultRequest>> cacheRequests=new HashMap<String, List<ResultRequest>>();
	private static HashMap<String,List<ResultRequest>> tempRequests= new HashMap<String, List<ResultRequest>>();
	public static TreeMap<byte[],List<byte[]>> statistics = new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_RAWCOMPARATOR);
	//private static HashMap<Integer,List<ResultRequest>> requestPerQuery= new HashMap<Integer, List<ResultRequest>>();
	public static int queryId=0;
	
	public CacheController() {
	}

	public void run() {
		FileOutputStream out=null;
		try {
			out = new FileOutputStream("/opt/Mylogs");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String address = "master";
		String t = "L10";
		String user = "root";
		Configuration hconf = HBaseConfiguration.create();
		hconf.set("hbase.rpc.timeout", "3600000");
		hconf.set("zookeeper.session.timeout", "3600000");

		/*try {
			CachingExecutor.connectTable(t, hconf);
			Scan scan = new Scan();
			//scan.setLoadColumnFamiliesOnDemand(true);
			scan.addFamily(Bytes.toBytes("S"));
			scan.addFamily(Bytes.toBytes("T"));
			HTable table = new HTable(hconf, t);
			Iterator<Result> it = table.getScanner(scan).iterator();
			int sum=0;
			while(it.hasNext()){
				Result res = it.next();
				List<byte[]> l = statistics.get(res.getRow());
				if(l==null){
					l = new ArrayList<byte[]>();
					statistics.put(res.getRow(), l);
				}
				for(KeyValue kv : res.list()){

					sum+=1;

					byte[] b = kv.getValue();
					byte[] v = new byte[b.length+2];
					v[0] = kv.getFamily()[0];
					if(kv.getQualifier().length>0)
						v[1]=kv.getQualifier()[0];
					else
						v[1]=(byte)0;
					for (int i = 0; i < b.length; i++) {
						v[i+2] = b[i];
					}
					l.add(v);
					//if(sum%10000==0){
					//	System.out.println("Statistics: "+sum);
					//}
				}
				
				
			}
			System.out.println("Statistics: "+sum+" map size:"+statistics.size());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		
		if(QueueDP.cacheResults){
			while(true){
				try {
					System.out.println("start");
					Thread.sleep(200000);
					String prolog = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
							"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
							"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+
							"PREFIX yago: <http://www.w3.org/2000/01/rdf-schema#>"+
							"PREFIX arco: <http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.owl#>";
					String NL = System.getProperty("line.separator") ;

					CachingExecutor.connectTable(t, hconf);
	            	int id=0;
	                synchronized (SyncPrimitive.itemId) {
	                	id=SyncPrimitive.itemId;
	                	SyncPrimitive.itemId++;
					}
			        CachingExecutor executor = new CachingExecutor(t,id);
					String q1 = "SELECT  * " +
							"WHERE   { ?x ub:takesCourse ?v3 ." +
							"?x rdf:type ub:GraduateStudent ."+
							"} order by ?v3";
					String q2 = "SELECT  * " +
					        "WHERE   { " +
					        "?x rdf:type ?v3 ." +
					        "?y rdf:type ub:University ." +
					        "?z rdf:type ub:Department ." +
					        "?x ub:memberOf ?z ." +
					        "?z ub:subOrganizationOf ?y ." +
					        "?x ub:undergraduateDegreeFrom ?y " +
					        "} order by ?v3";
					String q3 = "SELECT  ?x " +
							"WHERE   { ?x ub:publicationAuthor ?v3 ." +
							"?x rdf:type ub:Publication ."+
							"} order by ?v3";
					String q4 = "SELECT * "+
							"WHERE   { ?x ub:worksFor ?v3 ." +
							"?x rdf:type ub:AssistantProfessor ."+
							"?x ub:name ?n ." +
							"?x ub:emailAddress ?em ." +
							"?x ub:telephone ?t " +
							"} order by ?v3";
					String q5 = "SELECT  * " +
							"WHERE   { ?x ub:memberOf ?v3 ." +
							"?x rdf:type ub:UndergraduateStudent ."+
							"} order by ?v3";
					String q7 = "SELECT  *" +
					        "WHERE   { " +
					        "?x rdf:type ub:GraduateStudent ." +
					        "?y rdf:type ub:GraduateCourse ." +
					        "?v3 ub:teacherOf ?y ." +
					        "?x ub:takesCourse ?y " +
					        "} order by ?v3";
					String q9 = "SELECT  * " +
					        "WHERE   { ?x rdf:type ?v1 ." +
					        "?z rdf:type ?v2 ." +
					        "?y rdf:type ?v3 ."+
					        "?x ub:advisor ?z ." +
					        "?x ub:takesCourse ?y ." +
					        "?z ub:teacherOf ?y } order by ?v1 ?v2 ?v3";
					String q10 = "SELECT  * " +
								"WHERE   { ?p rdf:type ?tp . " +
									"?p ub:worksFor ?d . " +
									"?p ub:teacherOf ?c . " +
									"?s ub:takesCourse ?c } ";
					String inp=prolog + NL +q9;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    Query query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					Thread.sleep(10000);
					
					inp=prolog + NL +q2;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					Thread.sleep(10000);
					
					inp=prolog + NL +q5;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					Thread.sleep(10000);
					
					
					inp=prolog + NL +q4;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					Thread.sleep(10000);
					inp=prolog + NL +q7;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					Thread.sleep(10000);
					inp=prolog + NL +q3;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					Thread.sleep(10000);
					inp=prolog + NL +q1;
					out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.write(inp.getBytes());
					//System.out.println(inp);
				    query = QueryFactory.create(inp) ;
			
				    executor.executeQuery(query, false, true);
					out.write(("\n"+(new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
					out.flush();
					
					
					/*PriorityQueue<ResultRequest> reqQue = new PriorityQueue<ResultRequest>();
					for(Entry<String, List<ResultRequest>> e : cacheRequests.entrySet()){
						for(ResultRequest tr : e.getValue()){
							reqQue.add(tr);
						}
					}
					boolean executed=false;
					int t1=0;
					ResultRequest maxReq = null;
					for(ResultRequest mR : reqQue){
					//ResultRequest maxReq = reqQue.peek();
					//System.out.println("MaxCacheRequest: "+maxReq);
						if(mR!=null){
							if(mR.queries.cardinality()>1){//execute query
								//maxReq=reqQue.poll();
								reqQue.remove(mR);
								maxReq=mR;
								//out.write(("MaxCacheRequest: "+mR+"\n").getBytes());
								//out.flush();
								CachingExecutor.connectTable(t, hconf);
				            	int id=0;
				                synchronized (SyncPrimitive.itemId) {
				                	id=SyncPrimitive.itemId;
				                	SyncPrimitive.itemId++;
								}
						        CachingExecutor executor = new CachingExecutor(t,id);
								String inp = mR.getSPARQLQuery();
								out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
								out.write(inp.getBytes());
								//System.out.println(inp);
							    Query query = QueryFactory.create(inp) ;
						
							    executor.executeQuery(query, false, true);
								out.write(((new Timestamp(System.currentTimeMillis())).toString()+"\n").getBytes());
								out.flush();
							    executed=true;
							    break;
							}
						}
						t1++;
						if(t1>=50){
							break;
						}
					}
					//System.out.println("CacheRequests: ");
					//out.write(("CacheRequests: \n").getBytes());
					//out.flush();
					HashMap<String,List<ResultRequest>> tempCaheRequests = new HashMap<String, List<ResultRequest>>();
					int i=0;
					while(i<1000) {
						ResultRequest tr = reqQue.poll();
						if(tr==null)
							break;
						if(executed){
							float prevCard = tr.queries.cardinality();
							tr.queries.andNot(maxReq.queries);
							float currCard = tr.queries.cardinality();
							if(tr.queries.cardinality()==0)
								continue;
							float reduction = currCard/prevCard;
							if(reduction<1){
								reduction = 0;
								continue;
							}
							tr.benefit*=reduction;
						}
						
						//System.out.println(t);
						//out.write((tr+"\n").getBytes());
						//out.flush();
						List<ResultRequest> req = tempCaheRequests.get(tr.label);
						if(req==null){
							req = new ArrayList<ResultRequest>();
							req.add(tr);
							tempCaheRequests.put(tr.label, req);
						}
						else{
							req.add(tr);
						}
					}
					cacheRequests = tempCaheRequests;
					//System.out.println("ResultCache");
					//System.out.println(resultCache.keySet());
					//out.write(("ResultCache\n").getBytes());
					//out.write((resultCache.keySet()+"\n").getBytes());
					//out.flush();
					
					Thread.sleep(30000);*/
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else{
		
			while(true){
				try {
					Thread.sleep(200000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}

	public static void cache(String label, CachedResult result) {
		CachedResults results=resultCache.get(label);
		if(results==null){
			results = new CachedResults();
			results.results.add(result);
			resultCache.put(label, results);
		}
		else{
			boolean found=false;
			for(CachedResult cr : results.results){
				cr.clearTempData();
				if(cr.equals(result)){
					found=true;
					break;
				}
			}
			if(!found)
				results.results.add(result);
		}
	}

	public static void request(OptimizeOpVisitorDPCaching visitor, String label,HashMap<Integer, Long> selectiveBindings,TreeMap<Integer, Integer> canonicalVarMapping, BitSet edges, Double cost, BitSet joinOrder) {
		//specify ordering
		TreeMap<Long,List<Integer>> requestOrdering = new TreeMap<Long, List<Integer>>();
		for(Entry<Integer, Long> e : selectiveBindings.entrySet()){
			List<Integer> t = requestOrdering.get(e.getValue());
			if(t==null){
				t = new ArrayList<Integer>();
				int canonId=0;
				for(Entry<Integer, Integer> c : canonicalVarMapping.entrySet()){
					if(c.getValue().equals(e.getKey())){
						canonId = c.getKey();
						break;
					}
				}
				t.add(canonId);
				requestOrdering.put(e.getValue(), t);
			}
			else{
				int canonId=0;
				for(Entry<Integer, Integer> c : canonicalVarMapping.entrySet()){
					if(c.getValue().equals(e.getKey())){
						canonId = c.getKey();
						break;
					}
				}
				t.add(canonId);
			}
			
		}
		List<Integer> ordering = new ArrayList<Integer>();
		for(Entry<Long,List<Integer>> e : requestOrdering.entrySet()){
			for(Integer i : e.getValue()){
				ordering.add(i);
			}
		}
		Collections.sort(ordering);
		List<Integer> ordering2 = new ArrayList<Integer>();
		for (int i = joinOrder.nextSetBit(0); i >= 0; i = joinOrder.nextSetBit(i+1)) {
			int canonId=0;
			for(Entry<Integer, Integer> c : canonicalVarMapping.entrySet()){
				if(c.getValue().equals(i)){
					canonId = c.getKey();
					break;
				}
			}
			ordering2.add(canonId);
		}
		Collections.sort(ordering2);
		float coverage = ((float)edges.cardinality())/((float)visitor.numTriples);
		if(ordering.size()>0)
			coverage = coverage+(float)(ordering.size()/(float)visitor.numTriples);
		//System.out.println("Request label:"+ label+ " coverage:"+coverage);
		List<ResultRequest> req = tempRequests.get(label);
		if(req==null){
			req = new ArrayList<ResultRequest>();
			ResultRequest r = new ResultRequest(ordering, new Double(coverage), cost, label);
			r.queries.set(queryId);
			req.add(r);
			tempRequests.put(label, req);
		}
		else{
			/*ResultRequest r = new ResultRequest(ordering, new Double(coverage), cost, label);
			r.queries.set(queryId);
			req.add(r);*/
		}
		
		/*for(Integer in : ordering2){
			List<Integer> o = new ArrayList<Integer>();
			o.addAll(ordering);
			o.add(in);
			
			coverage = ((float)edges.cardinality())/((float)visitor.numTriples);
			if(o.size()>0)
				coverage = coverage+(float)3.0;
			//System.out.println("Request label:"+ label+ " coverage:"+coverage);
			req = tempRequests.get(label);
			if(req==null){
				req = new ArrayList<ResultRequest>();
				ResultRequest r = new ResultRequest(o, new Double(coverage), cost, label);
				r.queries.set(queryId);
				req.add(r);
				tempRequests.put(label, req);
			}
			else{
				ResultRequest r = new ResultRequest(o, new Double(coverage), cost, label);
				r.queries.set(queryId);
				req.add(r);
			}
		}*/
	}

	public static void newQuery() {
		tempRequests= new HashMap<String, List<ResultRequest>>();
	}

	public static void finishQuery(Double cost, double maxCoverage, OptimizeOpVisitorDPCaching visitor) {
		if(tempRequests.containsKey("$1_113_$2&$1_176_$3&$1_298_$4&$1_301_$5&$1_407_$6&$1_85_$7&")){
			queryId++;
			return;
		}
		//List<ResultRequest> reqPerId = new ArrayList<ResultRequest>();
		for(Entry<String, List<ResultRequest>> e : tempRequests.entrySet()){
			for(ResultRequest t : e.getValue()){
			//ResultRequest t = e.getValue().get(0);
				if(t.benefit>maxCoverage){
					//if(resultCache.get(t.label)!=null)
					//	continue;
					t.benefit*=cost;
					t.computeBenefitPerCost();
					t.setVisitor(visitor);
					List<ResultRequest> req = cacheRequests.get(e.getKey());
					if(req==null){
						req = new ArrayList<ResultRequest>();
						req.add(t);
						cacheRequests.put(e.getKey(), req);
						//reqPerId.add(t);
					}
					else{
						String o = "";
						for(Integer i : t.ordering){
							o+=i+"_";
						}
						ResultRequest f = null;
						for(ResultRequest r : req){
							String co = "";
							for(Integer i : r.ordering){
								co+=i+"_";
							}
							if(co.equals(o)){
								f=r;
								break;
							}
						}
						if(f==null){
							req.add(t);
							//reqPerId.add(t);
						}
						else{
							f.cost=t.cost;
							f.queries.or(t.queries);
							f.benefit+=t.benefit;
							f.computeBenefitPerCost();
							f.setVisitor(visitor);
							//reqPerId.add(f);
						}
					}
				}
			}
		}
		//requestPerQuery.put(queryId,reqPerId);
		queryId++;
	}


	
}
