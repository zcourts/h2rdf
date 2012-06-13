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
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;

import concurrent.Barrier;
import concurrent.Queue;

public class SyncPrimitive implements Watcher {
	static public final String NL = System.getProperty("line.separator") ; 
    static ZooKeeper zk = null;
    static Integer mutex;
    static Integer mutex1;
    static int finished;
    static int execute;
    static boolean dead;
    String output, input;

    SyncPrimitive(String address) {
        if(zk == null){
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
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
    }
    
    public static void main(String args[]) {
        if (args[0].equals("qTest")){
            Queue q = new Queue(args[1], "/in", "/out");
	        if (args[2].equals("p")) {
		    	Producer p =new Producer(args[1],q,zk);
	            long startTime = new Date().getTime();
	        	p.produce(args);
	            long stopTime = new Date().getTime();
	            System.out.println("Total time in ms: "+ (stopTime-startTime));
	        }
	        else if (args[2].equals("c")){
	        	consume(q);
	        }
        }

    }

    public static void consume(Queue q) {
        System.out.println("Consumer");
        int r=0;
        for (; ;) {
            try{
                int r1 = q.consume();
                r+=r1;
                System.out.println("Item: " + r);
            } catch (KeeperException e){
            	
            } catch (InterruptedException e){

            }
        }
    }
}
