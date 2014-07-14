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
package gr.ntua.h2rdf.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class TranslateIndexEndpoint extends BaseEndpointCoprocessor
implements TranslateIndexProtocol {

	@Override
	public List<byte[]> translate(List<byte[]> list)
			throws IOException {

		List<byte[]> ret = new ArrayList<byte[]>();
		Iterator<byte[]> itl = list.iterator();
		if(itl.hasNext()){
			byte[] tr = itl.next();
			Scan scan = new Scan(tr);
			InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion().getScanner(scan);
			try {
				 List<KeyValue> curVals = new ArrayList<KeyValue>();
				 boolean hasMore = false;
				 int comp, count=0;;
				 do {
				    curVals.clear();
					hasMore = scanner.next(curVals);
					Iterator<KeyValue> it = curVals.iterator();
					while(it.hasNext()){
						KeyValue kv = it.next();
						while((comp = Bytes.compareTo(kv.getRow(), tr))>0){
							ret.add(new byte[0]);
							//System.out.println("not found");
							if(itl.hasNext()){
								tr = itl.next();
							}
							else{
								 scanner.close();
								 return null;
							}
						}
						if(comp ==0){
							ret.add(kv.getValue());
							count++;
							//System.out.println(Bytes.toString(kv.getRow()));
							if(itl.hasNext()){
								tr = itl.next();
							}
							else{
								 scanner.close();
								 return ret;
							}
						}
					}
				 } while (hasMore);
				 System.out.println(count);
			} finally {
				 scanner.close();
			}
		}
		
		return ret;
	}

}
