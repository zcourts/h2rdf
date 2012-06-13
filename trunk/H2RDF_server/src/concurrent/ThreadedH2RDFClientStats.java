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
package concurrent;

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
