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

import gr.ntua.h2rdf.dpplanner.CacheController;

import java.io.IOException;
import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class SyncPrimitive implements Watcher {
	static public final String NL = System.getProperty("line.separator") ; 
    static ZooKeeper zk = null;
    static Integer mutex;
    static Integer mutex1;
    static int finished;
    static int execute;
    static boolean dead;
    public static Integer itemId;
    String output, input, address;

    SyncPrimitive(String address) {
    	this.address =address;
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 2000000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
		    //System.out.println("Process: " + event.getType());
		    mutex.notify();
		}
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
                    zk = new ZooKeeper(address, 3000, this);
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
    }
    
    public static void main(String args[]) {
        if (args[0].equals("qTest")){
            Queue q = null;
	        if (args[2].equals("p")) {
		    	Producer p =new Producer(args[1],q,zk);
	            long startTime = new Date().getTime();
	        	p.produce(args);
	            long stopTime = new Date().getTime();
	            System.out.println("Total time in ms: "+ (stopTime-startTime));
	        }
	        else if (args[2].equals("dp")){
	        	CacheController c = new CacheController();
	        	c.start();
	            q = new QueueDP(args[1], "/in", "/out",args[3]);
	        	consume(q);
	        }
	        else if (args[2].equals("c")){
	            q = new Queue(args[1], "/in", "/out");
	        	consume(q);
	        }
        }

    }

    public static void consume(Queue q) {
        System.out.println("Consumer");
        itemId=0;
        for (; ;) {
            try{
            	int id=0;
                synchronized (itemId) {
                	id=itemId;
                    itemId++;
				}
                int r1 = q.consume(id);
                System.out.println("Item: " + itemId);
            } catch (KeeperException e){
            	
            } catch (InterruptedException e){

            }
        }
    }
    
}
