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
package gr.ntua.h2rdf.concurrent;

/**
 * A simple example program to use DataMonitor to start and
 * stop executables based on a znode. The program watches the
 * specified znode and saves the data that corresponds to the
 * znode in the filesystem. It also starts the specified program
 * with the specified arguments when the znode exists and kills
 * the program if the znode goes away.
 */
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class Executor
    implements Watcher
{
	
    String root;
    int deleted;

    boolean dead;
    ZooKeeper zk;
    String filename;
    Process child;
    static Integer mutex;
    public long startTimeReal;
    public static long startTime;
	private static String separator = "$query$" ;
    
    public Executor(String address, String root, 
            String query) throws KeeperException, IOException {
    	this.root=root;
    	dead=false;
    	filename="";
        zk = new ZooKeeper(address, 3000, this);
        startTimeReal = new Date().getTime();
        byte[] value;
        value = Bytes.toBytes(query);
        byte[] v = new byte[value.length+1];
        v[0]=(byte) 0;
        for (int i = 1; i < v.length; i++) {
			v[i]=value[i-1];
		}
        try {
			String f=zk.create(root + "/element", v, Ids.OPEN_ACL_UNSAFE,
			            CreateMode.PERSISTENT_SEQUENTIAL);
			filename = "/out/"+f.split("/")[2];
			System.out.println(filename);
	        zk.exists(filename, this);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
		startTime = new Date().getTime();
        if (args.length < 3) {
            System.err
                    .println("USAGE: Executor address root query");
            System.exit(2);
        }
        String NL = System.getProperty("line.separator") ; 
        String address = args[0];
        String root = args[1];
        String table = args[2];
        String query = args[3];
        String prolog = "PREFIX dc: <http://dbpedia.org/resource/>"+
		"PREFIX p: <http://dbpedia.org/ontology/>"+
		"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
		"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
		"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>";

		String q = table+ separator +prolog + NL + query;
		
        try {
            new Executor(address, root, q).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {

        try {
            synchronized (this) {
                while (!dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            long stopTime = new Date().getTime();
            System.out.println("Total time in ms: "+ (stopTime-startTime));
            System.out.println("Real time in ms: "+ (stopTime-startTimeReal));
        }
        long stopTime = new Date().getTime();
        System.out.println("Total time in ms: "+ (stopTime-startTime));
        System.out.println("Real time in ms: "+ (stopTime-startTimeReal));
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
        }
        else{
	        if(path.equals(filename) && (event.getType() == Event.EventType.NodeCreated)){
	            // It's all over
	        	System.out.println("Finished ");
	        	try {
		        	dead=true;
		            synchronized (this) {
		                notifyAll();
		            }
					zk.delete(filename, 0);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
        }
        
        /*if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with 
                // server and any watches triggered while the client was 
                // disconnected will be delivered (in order of course)
                break;
            case Expired:
                // It's all over
            	dead=true;
                synchronized (this) {
                    notifyAll();
                }
                break;
            }
        } else {
            if (path != null && path.equals(filename)) {
                // Something has changed on the node, let's find out
                zk.exists(filename, true, this, null);
            }
        }*/
    }

   /* public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;

        if(rc==Code.NoNode){
        	dead=true;
            synchronized (this) {
                notifyAll();
            }
        	return;
        }
        zk.exists(filename, true, this, null);
        return;

    }*/
}

