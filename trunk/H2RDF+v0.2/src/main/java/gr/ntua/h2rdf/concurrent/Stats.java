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
package gr.ntua.h2rdf.concurrent;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Stats {

    static int finished, total, prevOut=0;
    static ZooKeeper zk;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
        Queue q = new Queue(args[0], "/in", "/out");
        FileSystem fs=null;
		if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(args[0], 3000, q);
                System.out.println("Finished starting ZK: " + zk);
    			Configuration conf = new Configuration();
				fs = FileSystem.get(conf);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
		int prevOut=0;
		try {
			prevOut = zk.getChildren("/out", false).size();
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        long outstart=System.currentTimeMillis();
		 while(true){
			 try {
				Thread.sleep(1000);
            	//int temp=zk.getChildren("/out", true).size();
            	
        		int qlen=zk.getChildren("/in", false).size();
            	long st1 = System.currentTimeMillis();
        		int temp = zk.getChildren("/out", false).size();
        		/*String outfile, p;
	            Stat stat = null;
        		while(it.hasNext()){
        			p="/out/"+it.next();
					outfile = Bytes.toString(zk.getData(p,false, stat));
        			zk.delete(p, 0);
        			//fs.delete(new Path(outfile), true);
        			temp++;
        		}*/
				double out_t = (double) (temp-prevOut)*1000/(double)(st1-outstart);
        		prevOut=temp;
        		String[] ganglia_real_throughput_command = new String[]{"gmetric", "-n",
            			"out_real_THROUGHPUT", "-v", Double.toString(out_t), "-d","600" ,"-t",
            			"double" , "-u",  "qps"};
	    		Runtime.getRuntime().exec(ganglia_real_throughput_command);

        		
        		System.out.println("Real output throughput: "+out_t+" queue: "+qlen);
        		ganglia_real_throughput_command = new String[]{"gmetric", "-n",
            			"qlen", "-v",""+qlen, "-d","600" ,"-t",
            			"double" , "-u",  "queries"};
	    		Runtime.getRuntime().exec(ganglia_real_throughput_command);
        		outstart=st1;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 }
	}
}
