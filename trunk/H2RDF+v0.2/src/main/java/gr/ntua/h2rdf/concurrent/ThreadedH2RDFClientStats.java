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
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class ThreadedH2RDFClientStats extends Thread{
	private ZooKeeper zk;
	private FileSystem fs;
	public ThreadedH2RDFClientStats(ZooKeeper zk, FileSystem fs) {
		super("ThreadedClientStats");
		this.zk=zk;
		this.fs=fs;
	}
	
	public void run() {
		long outstart=System.currentTimeMillis();
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
		 while(true){
			 try {
				Thread.sleep(10000);
            	long st1 = System.currentTimeMillis();
            	//int temp=zk.getChildren("/out", true).size();
            	
        		Iterator<String> it = zk.getChildren("/out", false).iterator();
        		int temp = 0;
        		while(it.hasNext()){
        			zk.delete("/out/"+it.next(), 0);
        			temp++;
        		}
    			fs.delete(new Path("output"), true);
				double out_t = (double) (temp-prevOut)*1000/(double)(st1-outstart);
        		prevOut=0;
        		String[] ganglia_real_throughput_command = new String[]{"gmetric", "-n",
            			"out_real_THROUGHPUT", "-v", Double.toString(out_t), "-d","600" ,"-t",
            			"double" , "-u",  "qps"};
	    		Runtime.getRuntime().exec(ganglia_real_throughput_command);

        		int qlen=zk.getChildren("/in", false).size();
        		System.out.println("Real output throughput: "+out_t+" queue: "+qlen);
        		ganglia_real_throughput_command = new String[]{"gmetric", "-n",
            			"qlen", "-v",""+qlen, "-d","600" ,"-t",
            			"double" , "-u",  "queries"};
	    		Runtime.getRuntime().exec(ganglia_real_throughput_command);
        		outstart=System.currentTimeMillis();
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
