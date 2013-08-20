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
package gr.ntua.h2rdf.loadTriples;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class Pair implements Comparable<Pair> {

	private Integer key;
	private ImmutableBytesWritable value;
	
	public Pair(int key, ImmutableBytesWritable value) {
		this.key =key;
		this.value = value;
	}
	public Integer getKey() {
		return key;
	}
	public void setKey(Integer key) {
		this.key = key;
	}
	public ImmutableBytesWritable getValue() {
		return value;
	}
	public void setValue(ImmutableBytesWritable value) {
		this.value = value;
	}
	
	@Override
	public int compareTo(Pair o) {
		Integer k = o.getKey();
		ImmutableBytesWritable v = o.getValue();
		int r = k.compareTo(key);
		if(r!=0){
			return r;
		}
		return v.compareTo(value);
	}

}
