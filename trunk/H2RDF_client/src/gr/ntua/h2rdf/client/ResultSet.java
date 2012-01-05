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


public class ResultSet {
	//private String outfolder;
	private Path[] outfiles;
	private Path o;
	private int nextfile, filesNo;
	private BufferedReader outfile;
	private FileSystem fs;
	
	public ResultSet(String out) {
		//System.out.println(out);
		Configuration conf = new Configuration();
		//System.out.println(conf.get("fs.default.name"));
		try {
			try {
				//fs = FileSystem.get(new URI(conf.get("fs.default.name")), conf, "arcomem");
				fs = FileSystem.get(new URI(conf.get("fs.default.name")), conf, "npapa");
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
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Result getNext() {
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
		
		Result res = new Result(line);
		return res;
	}

	public void close() {
		try {
			fs.delete(o, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
