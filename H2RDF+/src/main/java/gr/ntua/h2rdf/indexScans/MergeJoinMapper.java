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

import gr.ntua.h2rdf.inputFormat2.MultiTableInputFormat;
import gr.ntua.h2rdf.inputFormat2.MultiTableInputFormatBase;
import gr.ntua.h2rdf.inputFormat2.TableRecordGroupReader;
import gr.ntua.h2rdf.inputFormat2.TableRecordReader;
import gr.ntua.h2rdf.inputFormat2.TableRecordReader2;
import gr.ntua.h2rdf.loadTriples.SortedBytesVLongWritable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MergeJoinMapper extends Mapper<BytesWritable, BytesWritable, Bindings, BytesWritable> {
	
	private boolean table;
	private boolean file;
	private Scan scan;
	private byte joinVar;
	private byte pattern;
	private Scan[] scans;

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		int countStats=0;
		Map<Byte,Long> stats = new HashMap<Byte, Long>();
		
		TableSplit split = (TableSplit)context.getInputSplit();
		int joinVar = split.getScan().getAttribute("joinVar")[0];
		
		List<TableRecordGroupReader> scanners = new ArrayList<TableRecordGroupReader>();
		MultiTableInputFormat in = new MultiTableInputFormat();
		Scan s = split.getScan();
		
		s.setStartRow(split.getStartRow());
		s.setStopRow(split.getEndRow());
		int numpats = context.getConfiguration().getInt("h2rdf.inputPatterns", 0);
		int groups = context.getConfiguration().getInt("h2rdf.inputGroups", 0);
		System.out.println("h2rdf.inputGroups     "+ groups);
		Map<Integer,TableRecordGroupReader> m = new HashMap<Integer, TableRecordGroupReader>();
		TableRecordGroupReader reader = new TableRecordGroupReader(Bytes.toString(split.getTableName()));
		//m.put(Bytes.toInt(s.getAttribute("group")), reader);
		reader.addScan(s);
		//TableRecordReader2 reader = new TableRecordReader2(s,split.getTableName());
		long startKey=0;
		if(reader.nextKeyValue()){
			scanners.add(reader);
			startKey = reader.getJvar();
		}
		//System.out.println("Start key: "+startKey);
		
		
		for (int i = 0; i < numpats; i++) {

			String s1 = context.getConfiguration().get("h2rdf.externalScans_"+i, null);
			if(s1==null)
				continue;

		    ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(s1));
		    DataInputStream dis = new DataInputStream(bis);
		    Scan scan = new Scan();
		    scan.readFields(dis);

			TableRecordGroupReader val = m.get(Bytes.toInt(scan.getAttribute("group")));
			if(val==null){
				val = new TableRecordGroupReader(Bytes.toString(scan.getAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME)));//Bytes.toString(split.getTableName()));
				val.addScan(scan);
				m.put(Bytes.toInt(scan.getAttribute("group")), val);
			}
			else{
				val.addScan(scan);
			}

			//TableRecordReader2 trr = new TableRecordReader2(scan, split.getTableName(), startKey);
			//if(trr.nextKeyValue()){
			//	scanners.add(trr);
			//}
		}
		for(Entry<Integer, TableRecordGroupReader> e :m.entrySet()){
			if(e.getValue().nextKeyValue()){
				scanners.add(e.getValue());
			}
		}
		numpats++;
		while(scanners.size()==groups){
			List<TableRecordGroupReader> nextScanners = new ArrayList<TableRecordGroupReader>();
			Iterator<TableRecordGroupReader> it = scanners.iterator();
			TableRecordGroupReader sf = it.next();
			long maxKey=sf.getJvar(), firstKey = sf.getJvar();
			//System.out.println();
			//System.out.print(firstKey+" ");
			int i=1;
			while(it.hasNext()){
				TableRecordGroupReader s1 = it.next();
				long key = s1.getJvar();
				//System.out.print(key+" ");
				if(key> maxKey){
					maxKey = key;
				}
				if(key == firstKey){
					i++;
				}
			}
			//System.out.println("maxKey:"+maxKey);
			it = scanners.iterator();
			if(i == groups){//key passed
				
				Bindings b = new Bindings();

				List<Bindings> lres = new ArrayList<Bindings>();
				int first=0;
				
				while(it.hasNext()){
					TableRecordGroupReader s1 = it.next();
					Bindings bk = s1.getCurrentKey();
					if(s1.nextKeyValue())
						nextScanners.add(s1);
					else{
						//System.out.println("Closing");
						s1.close();
					}
					
					List<Bindings> templ = new ArrayList<Bindings>();
					templ.add(bk);
					//System.out.println(bk.map);
					if(first==0){
						first++;
						lres=templ;
						continue;
					}
					lres = Bindings.merge(lres,templ);
					//b.addAll(bk);
				}
				
				for(Bindings b1 : lres){
					//System.out.println("Output: "+b1.map);
					//b1.print(new HTable(HBaseConfiguration.create(), "YAGO_Index"));
					for(Entry<Byte, Set<Long>> e1 : b1.map.entrySet()){
						if(countStats==0){
							stats.put(e1.getKey(), new Long(e1.getValue().size()));
						}
						else{
							Long st = stats.get(e1.getKey());
							stats.put(e1.getKey(), new Long(st+e1.getValue().size()));
						}
					}
					countStats++;
					b1.writeOut(context,0, new Bindings());

					//context.write(b1, new BytesWritable(new byte[0]));
				}
				//context.write(b, new BytesWritable());
			}
			else{ //move all scanners to maxKey
				while(it.hasNext()){
					TableRecordGroupReader s1 = it.next();
					if(s1.getJvar() < maxKey){
						if(s1.goTo(maxKey))
							nextScanners.add(s1);
						else
							s1.close();
					}
					else{
						nextScanners.add(s1);
					}
				}
			}
			scanners = nextScanners;
			
		}
		
		for(TableRecordGroupReader r : scanners){
			r.close();
		}
		Counter c = context.getCounter("h2rdf", "sample");
		c.increment(countStats);
		
		for(Entry<Byte, Long> e : stats.entrySet()){
			c = context.getCounter("h2rdf", e.getKey().intValue()+"");
			c.increment(e.getValue());
			//System.out.println(e.getKey().intValue()+" "+e.getValue());
		}
		
		//context.getOutputCommitter().commitTask(context);
		//cleanup(context);
		/*while(reader.nextKeyValue()){
			//System.out.println(reader.getCurrentKey().map);
			context.write(reader.getCurrentKey(), NullWritable.get());
		}
		reader.close();*/
		
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

	
	
}
