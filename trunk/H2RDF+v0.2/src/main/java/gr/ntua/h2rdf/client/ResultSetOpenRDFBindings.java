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
package gr.ntua.h2rdf.client;

import gr.ntua.h2rdf.indexScans.Bindings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.SequenceFile;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.query.impl.MapBindingSet;

public class ResultSetOpenRDFBindings implements QueryResult<BindingSet> {
	//private String outfolder;
	private Path[] outfiles;
	private Path o;
	private int nextfile, filesNo;
	private SequenceFile.Reader outfile;
	private FileSystem fs;
	private HTable table;
	private BindingsResultIterable queryResult;
	private boolean lineFinished=true;
	private H2RDFConf hconf;
	private HashMap<Integer, String> varIds;
	
	public ResultSetOpenRDFBindings(String out, H2RDFConf hconf, HashMap<Integer, String> varIds) {
		//System.out.println(out);
		this.varIds=varIds;
		this.hconf=hconf;
		Configuration conf = hconf.getConf();// new Configuration();
		//System.out.println(conf.get("fs.default.name"));
		try {
			try {
				//Configuration c = HBaseConfiguration.create();
				this.table= new HTable( conf, hconf.getTable()+"_Index" );
				fs = FileSystem.get(new URI(conf.get("fs.default.name")), conf, hconf.getUser());
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(out.startsWith("output/")){
				Path p =new Path(out);
				o=p;
				if(fs.isFile(p)){//file
					outfiles= new Path[1];
					outfiles[0]=p;
					filesNo=1;
					nextfile=1;
					outfile = new SequenceFile.Reader(fs, p, conf);
					//outfile = fs.open(p);
				}
				else if(fs.exists(p)){//MapReduce folder
					Path[] outf = FileUtil.stat2Paths(fs.listStatus(p));
					int paths=0;
					outfiles= new Path[outf.length];
					for (Path f : outf) {
						if(f.getName().startsWith("part")){
							outfiles[paths]=f;
							paths++;
						}
					}
					filesNo=paths;
					nextfile=1;
					outfile = new SequenceFile.Reader(fs, outfiles[0], conf);
				}
			}
			else{
				o=null;
				filesNo=1;
				nextfile=1;
				//outfile = new SequenceFile.Reader(fs, p, conf);
				//outfile = new ByteArrayInputStream(out.getBytes());
			}
			lineFinished=true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws QueryEvaluationException {
		/*try {
			if(o!=null)
				fs.delete(o, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		if(lineFinished){
			/*if(hasRelabeling||hasSelective)
			b.readFields(key.getBytes(), varRelabeling,selectiveBindings);
		else
			b.readFields(key.getBytes());*/
			
			Bindings b = new Bindings();

			try {
				if(!outfile.next(b)){
					outfile.close();
					if(nextfile>=filesNo)
						return false;
					else{
						outfile = new SequenceFile.Reader(fs, outfiles[nextfile], hconf.getConf());
						nextfile++;
						return hasNext();
					}
				}
			} catch (IOException e) {
				nextfile++;
				return hasNext();
			}

	    	if(!b.valid)
				return hasNext();
			
			try {
				queryResult = new BindingsResultIterable(b, table,varIds);
			} catch (IOException e) {
				e.printStackTrace();
			}
			lineFinished = false;
			return true;
		}
		else{
			boolean ret = queryResult.hasNext();
			if(ret)
				return ret;
			else{
				lineFinished = true;
				return hasNext();
			}
		}
	}

	@Override
	public MapBindingSet next() throws QueryEvaluationException {
		try {
			return queryResult.next();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public String next1() throws QueryEvaluationException {
		String ret=null;
		try {
			ret = queryResult.next().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		System.out.println("Not supported opperation remove");
		throw new QueryEvaluationException();
	}

}
