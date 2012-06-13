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
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Remove {

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
            	
		Iterator<String> it;
		try {
			it = zk.getChildren("/out", false).iterator();
			String outfile, p;
	        Stat stat = null;
			while(it.hasNext()){
				p="/out/"+it.next();
				zk.delete(p, 0);
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
