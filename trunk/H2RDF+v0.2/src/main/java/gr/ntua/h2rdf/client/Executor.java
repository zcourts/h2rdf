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

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Executor 
implements Watcher
{
	 int deleted;

    private boolean dead;
    private ZooKeeper zk;
    private String filename, table, root, outfile;
    private H2RDFConf conf;
    static Integer mutex;
	String separator = "$query$" ;
    
    public Executor(String root, H2RDFConf conf) {
    	this.root=root;
    	this.conf=conf;
    	this.table =conf.getTable();
    	dead=false;
    	filename="";
        try {
			zk = new ZooKeeper(conf.getAddress(), 2000000, this);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ResultSet run(String query) throws Exception {
    	dead=false;
    	filename="";
    	outfile="";
    	byte[] value1 = Bytes.toBytes(table+separator+query);
    	byte[] value = new byte[value1.length+1];
    	value[0]=(byte) 0; 
    	for (int i = 0; i < value1.length; i++) {
			value[i+1]=value1[i];
		}
    	/*if(!zk.getState().isConnected()){
        	try {
        		zk.close();
				zk = new ZooKeeper(conf.getAddress(), 3000, this);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}*/
        long startTimeReal = new Date().getTime();
        try {
			String f=zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
			            CreateMode.PERSISTENT_SEQUENTIAL);
			filename = "/out/"+f.split("/")[2];
	        Stat st = zk.exists(filename, this);
	        if(st!=null){
	        	System.out.println("Finished ");
	        	try {
		        	dead=true;
		            Stat stat = null;
		            outfile = Bytes.toString(zk.getData(filename,false, stat));
		            //System.out.println(outfile +"dfsdfsd");
					zk.delete(filename, 0);
		            synchronized (this) {
		                notifyAll();
		            }
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
			System.out.println(filename);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
            synchronized (this) {
                while (!dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            long stopTime = new Date().getTime();
            System.out.println("Real time in ms: "+ (stopTime-startTimeReal));
        }
        long stopTime = new Date().getTime();
        //System.out.println(outfile);
        //if(outfile.startsWith("output/")){
	        System.out.println("Real time in ms: "+ (stopTime-startTimeReal));
	        ResultSet rs = new ResultSet(outfile, conf);
	        return rs;
        /*}
        else{
        	throw new Exception(outfile);
        }*/
    }
    
	public void process(WatchedEvent event) {
        //System.out.println("message");
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
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
            	try {
            		zk.close();
    				zk = new ZooKeeper(conf.getAddress(), 3000, this);
    			} catch (IOException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                break;
            }
        }
        else{
	        if(path.equals(filename) && (event.getType() == Event.EventType.NodeCreated)){
	            // It's all over
	        	System.out.println("Finished ");
	        	try {
		        	dead=true;
		            Stat stat = null;
		            outfile = Bytes.toString(zk.getData(filename,false, stat));
		            //System.out.println(outfile +"dfsdfsd");
					zk.delete(filename, 0);
		            synchronized (this) {
		                notifyAll();
		            }
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

	public void close() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
