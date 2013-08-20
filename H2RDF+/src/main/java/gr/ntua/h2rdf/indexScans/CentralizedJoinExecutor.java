/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.indexScans;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;

import com.hp.hpl.jena.sparql.core.Var;

public class CentralizedJoinExecutor {

	public static Path execute(JoinPlan plan, Var joinVar, HTable table,
			HTable indexTable) {

		
		Iterator<ResultBGP> it =  plan.Map.iterator();
		List<Scan> scans = new ArrayList<Scan>();
		while(it.hasNext()){
			ResultBGP temp = it.next();
			if(temp.getClass().equals(BGP.class)){
				scans.addAll(((BGP)temp).getScans("?"+joinVar.getVarName()));
				
			}
		}
		System.out.println(scans.size());
		/*Scan sc = scans.get(1);
		Iterator<Result> it1 = table.getScanner(sc).iterator();
		while(it1.hasNext()){
			Result res = it1.next();
			if(res.size()>0){
				byte[] temp = res.getRow();
				System.out.println(Bytes.toStringBinary(temp));
				long[] n = ByteTriple.parseRow(temp);
				String[] r = new String[3];
				SortedBytesVLongWritable v = new SortedBytesVLongWritable(n[0]);
				Get get = new Get(v.getBytesWithPrefix());
				get.addColumn(Bytes.toBytes("2"), new byte[0]);
				Result res1 = indexTable.get(get);
				r[0] = Bytes.toString(res1.value());

				v = new SortedBytesVLongWritable(n[1]);
				get = new Get(v.getBytesWithPrefix());
				get.addColumn(Bytes.toBytes("2"), new byte[0]);
				res1 = indexTable.get(get);
				r[1] = Bytes.toString(res1.value());
				
				v = new SortedBytesVLongWritable(n[2]);
				get = new Get(v.getBytesWithPrefix());
				get.addColumn(Bytes.toBytes("2"), new byte[0]);
				res1 = indexTable.get(get);
				r[2] = Bytes.toString(res1.value());
				System.out.println(r[0]+" "+r[1]+" "+r[2]);
			}
		}*/
		return null;
		
	}

	public static List<ResultBGP> execute(Map<Var, JoinPlan> m, HTable table,
			HTable indexTable) {
		
		return resultPlan(m,null);
	}
	
	private static List<ResultBGP> resultPlan(Map<Var, JoinPlan> m, Path path) {
		List<ResultBGP> ret = new ArrayList<ResultBGP>();
		for(Entry<Var, JoinPlan> e :m.entrySet()){
			JoinPlan plan = e.getValue();
	    	Set<Var> vars = new HashSet<Var>();
			for(ResultBGP b : plan.Map){
				vars.addAll(b.joinVars);
			}
			for(ResultBGP b : plan.Reduce){
				vars.addAll(b.joinVars);
			}
	    	ret.add(new ResultBGP(vars, path,null));
		}
		return ret;
	}

}
