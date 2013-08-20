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
package gr.ntua.h2rdf.inputFormat2;

import gr.ntua.h2rdf.indexScans.Bindings;
import gr.ntua.h2rdf.queryProcessing.QueryPlanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.BytesWritable;

public class TableRecordGroupReader {

	private HTable table;
	private List<TableRecordReader2> readers;
	private long startKey;
	private static BytesWritable value = new BytesWritable();
	private TableRecordReader2 minTrr ;

	public TableRecordGroupReader(String t,
			long startKey) throws IOException {
		table = QueryPlanner.tables.get(t);
		if(table==null){
			QueryPlanner.connectTableOnly(t, HBaseConfiguration.create());
			table = QueryPlanner.tables.get(t);
		}
		this.startKey = startKey;
		readers = new ArrayList<TableRecordReader2>();
		minTrr =null;
	}

	public TableRecordGroupReader(String t) throws IOException {
		table = QueryPlanner.tables.get(t);
		if(table==null){
			QueryPlanner.connectTableOnly(t, HBaseConfiguration.create());
			table = QueryPlanner.tables.get(t);
		}
		this.startKey = 0;
		readers = new ArrayList<TableRecordReader2>();
		minTrr =null;
	}
	
	public TableRecordGroupReader(HTable table, long startKey) {
		this.table=table;
		this.startKey = startKey;
		readers = new ArrayList<TableRecordReader2>();
		minTrr =null;
	}

	public TableRecordGroupReader(HTable table) {
		this.table=table;
		this.startKey = 0;
		readers = new ArrayList<TableRecordReader2>();
		minTrr =null;
	}

	public void addScan(Scan scan) throws IOException{
		if(startKey==0){
			TableRecordReader2 trr = new TableRecordReader2(scan, table);
			readers.add(trr);
		}
		else{
			TableRecordReader2 trr = new TableRecordReader2(scan, table,startKey);
			readers.add(trr);
		}
	}

	public Bindings getCurrentKey() {
		return minTrr.getCurrentKey();
	}


	public BytesWritable getCurrentValue() {
		return value;
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(readers.size()==0)
			return false;

		List<TableRecordReader2> nextReaders= new ArrayList<TableRecordReader2>();
		boolean found=false;
		if(minTrr==null){//first time 
			long minKey = Long.MAX_VALUE;
			for(TableRecordReader2 trr : readers){
				if(trr.nextKeyValue()){
					nextReaders.add(trr);
					if(trr.jVar<minKey){
						found =true;
						minKey=trr.jVar;
						minTrr=trr;
					}
				}
			}
			//key=minTrr.getCurrentKey();
			readers = nextReaders;
		}
		else{
			if(!minTrr.nextKeyValue()){//advance min and find new min
				readers.remove(minTrr);
			}
			long minKey = Long.MAX_VALUE;
			for(TableRecordReader2 trr : readers){
				if(trr.jVar<minKey){
					found =true;
					minKey=trr.jVar;
					minTrr=trr;
				}
			}
			
		}
		return found;
		
	}


	public boolean goTo(long key) throws IOException, InterruptedException {
		boolean found=false;
		long minKey = Long.MAX_VALUE;
		List<TableRecordReader2> nextReaders= new ArrayList<TableRecordReader2>();
		for(TableRecordReader2 trr : readers){
			if(trr.jVar<key){
				if(trr.goTo(key)){
					nextReaders.add(trr);
					if(trr.jVar<minKey){
						found =true;
						minKey=trr.jVar;
						minTrr=trr;
					}
				}
			}
			else{
				nextReaders.add(trr);
				if(trr.jVar<minKey){
					found =true;
					minKey=trr.jVar;
					minTrr=trr;
				}
			}
		}
		readers = nextReaders;
		return found;
		
	}


	public void close() throws IOException {
		for(TableRecordReader2 trr : readers){
			trr.close();
		}
	}

	public long getJvar() {
		return minTrr.jVar;
	}




}
