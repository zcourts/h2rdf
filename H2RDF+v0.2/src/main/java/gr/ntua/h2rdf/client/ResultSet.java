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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;



public class ResultSet {
	//private String outfolder;
	private Path[] outfiles;
	private Path o;
	private int nextfile, filesNo;
	private BufferedReader outfile;
	private FileSystem fs;
	private HTable table;
	
	public ResultSet(String out, H2RDFConf hconf) {
		//System.out.println(out);
		Configuration conf = hconf.getConf();//new Configuration();
		//System.out.println(conf.get("fs.default.name"));
		try {
			try {
				//Configuration c = HBaseConfiguration.create();
				this.table= new HTable( conf, hconf.getTable() );
				fs = FileSystem.get(new URI(conf.get("fs.default.name")), conf, hconf.getUser());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//fs.setWorkingDirectory(new Path("/user/arcomem/"));
			if(out.startsWith("output/")){
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
			}
			else{
				o=null;
				filesNo=1;
				nextfile=1;
				InputStream is = new ByteArrayInputStream(out.getBytes());
				outfile = new BufferedReader(new InputStreamReader(is));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public H2RDFQueryResult getNext() {
		String line=null;
		try {
			line = outfile.readLine();
			if(line == null){
				if(nextfile>=filesNo)
					return null;
				else{
					FSDataInputStream o = fs.open(outfiles[nextfile]);
					outfile = new BufferedReader(new InputStreamReader(o));
					nextfile++;
					return getNext();
				}
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		H2RDFQueryResult res = new H2RDFQueryResult(line, table);
		return res;
	}

	public void close() {
		try {
			if(o!=null)
				fs.delete(o, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
