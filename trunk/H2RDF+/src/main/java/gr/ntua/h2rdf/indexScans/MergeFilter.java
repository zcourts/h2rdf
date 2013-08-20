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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

public class MergeFilter implements Filter {
	private List<Scan> scans;
	
	public MergeFilter(List<Scan> scans) {
		this.scans =scans;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size =in.readInt();
		scans= new ArrayList<Scan>(size);
		for (int i = 0; i < size; i++) {
			Scan s = new Scan();
			s.readFields(in);
			scans.add(s);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(scans.size());
		for(Scan s : scans){
		    s.write(out);
		}
	}
	
	
	@Override
	public void reset() {
		
	}
	
	@Override
	public boolean filterAllRemaining() {
		return false;
	}

	@Override
	public ReturnCode filterKeyValue(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean filterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void filterRow(List<KeyValue> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean filterRowKey(byte[] arg0, int arg1, int arg2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public KeyValue getNextKeyHint(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasFilterRow() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFamilyEssential(byte[] arg0) {
		// TODO Auto-generated method stub
		return false;
	}


	@Override
	public KeyValue transform(KeyValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
