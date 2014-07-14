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
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class ApiExecutor
implements Watcher{

	 int deleted;

	 private boolean dead;
	 private ZooKeeper zk;
	 private String filename;
	 private final String table;
	 private final String root;
	 private String outfile;
	 private final H2RDFConf conf;
	 static Integer mutex;
	 private byte type;
	 private byte[] ret;
	 String separator = "$query$" ;

	public ApiExecutor(String root, H2RDFConf conf) {
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

	public void bulkPutTriples(String table, String triples) {
		String val = table+"$"+triples;
    	byte[] value=null;
		try {
			value = val.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//executeCall((byte) 5, value);
		executeAsyncCall((byte) 5, value);
	}


	public void bulkLoadTriples(String table) {
    	byte[] value=null;
		try {
			value = table.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//executeCall((byte) 6, value);
		executeAsyncCall((byte) 6, value);
	}

	
	private void executeAsyncCall(byte type, byte[] val) {
    	byte[] value = new byte[val.length+1];
    	value[0]= type;
    	this.type =type;
    	dead=false;
    	filename="";
    	outfile="";
    	for (int i = 0; i < val.length; i++) {
			value[i+1]=val[i];
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
        try {
			String f=zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
			            CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private byte[] executeCall(byte type, byte[] val) {
    	byte[] value = new byte[val.length+1];
    	value[0]= type;
    	this.type =type;
    	dead=false;
    	filename="";
    	outfile="";
    	for (int i = 0; i < val.length; i++) {
			value[i+1]=val[i];
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
        try {
			String f=zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
			            CreateMode.PERSISTENT_SEQUENTIAL);
			filename = "/out/"+f.split("/")[2];
	        Stat st = zk.exists(filename, this);
	        if(st!=null){
	        	//System.out.println("Finished ");
	        	try {
		        	dead=true;
		            Stat stat = null;
		            ret =zk.getData(filename,false, stat);
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
        }
        try {
			checkError();
		} catch (APIServerErrorException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;

	}

	private void checkError() throws APIServerErrorException{
		String r = new String(ret);
		if(r.startsWith("{\"error")){
			throw new APIServerErrorException(r);
		}
	}

	@Override
	public void process(WatchedEvent event) {
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
    				zk = new ZooKeeper(conf.getAddress(), 2000000, this);
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
	        	//System.out.println("Finished ");
	        	try {
		        	dead=true;
		            Stat stat = null;
		            ret = zk.getData(filename,false, stat);
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
	}

	private void parseResult(byte[] data) {
        outfile = Bytes.toString(data);
        System.out.println(outfile);
        ret = null;
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
