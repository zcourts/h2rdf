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
package gr.ntua.h2rdf.coprocessors;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import gr.ntua.h2rdf.indexScans.JoinPlan;

public interface TranslateIndexProtocol extends CoprocessorProtocol {
	
	public List<byte[]> translate(List<byte[]> list)
			throws IOException;

}
