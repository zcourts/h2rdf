/*******************************************************************************
 * Copyright (c) 2012 Nikos Papailiou.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Nikos Papailiou - initial API and implementation
 ******************************************************************************/
package gr.ntua.h2rdf.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResult;
import org.openrdf.query.impl.MapBindingSet;

public class ResultSetOpenRDF implements QueryResult<BindingSet> {
	//private String outfolder;
	private Path[] outfiles;
	private Path o;
	private int nextfile, filesNo;
	private BufferedReader outfile;
	private FileSystem fs;
	private HTable table;
	private H2RDFQueryResultItterable queryResult;
	private boolean lineFinished=true;
	
	
	public ResultSetOpenRDF(String out, H2RDFConf hconf) {
		//System.out.println(out);
		Configuration conf = new Configuration();
		//System.out.println(conf.get("fs.default.name"));
		try {
			try {
				Configuration c = HBaseConfiguration.create();
				this.table= new HTable( c, hconf.getTable() );
				fs = FileSystem.get(new URI(conf.get("fs.default.name")), conf, hconf.getUser());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//fs.setWorkingDirectory(new Path("/user/arcomem/"));
			Path p =new Path(out);
			o=p;
			if(fs.isFile(p)){//file
				outfiles= new Path[1];
				outfiles[0]=p;
				filesNo=1;
				nextfile=1;
				FSDataInputStream o = fs.open(p);
				outfile = new BufferedReader(new InputStreamReader(o));
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
				FSDataInputStream o = fs.open(outfiles[0]);
				outfile = new BufferedReader(new InputStreamReader(o));
			}
			lineFinished=true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws QueryEvaluationException {
		try {
			fs.delete(o, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		if(lineFinished){
			String line=null;
			try {
				line = outfile.readLine();
				if(line == null){
					if(nextfile>=filesNo)
						return false;
					else{
						FSDataInputStream o = fs.open(outfiles[nextfile]);
						outfile = new BufferedReader(new InputStreamReader(o));
						nextfile++;
						return hasNext();
					}
					
				}
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			queryResult = new H2RDFQueryResultItterable(line, table);
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
		return queryResult.next();
	}
	
	public String next1() throws QueryEvaluationException {
		String ret = queryResult.next().toString();
		return ret;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		System.out.println("Not supported opperation remove");
		throw new QueryEvaluationException();
	}

}
