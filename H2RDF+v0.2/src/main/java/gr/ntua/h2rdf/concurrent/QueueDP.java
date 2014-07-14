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

import gr.ntua.h2rdf.dpplanner.CachingExecutor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

public class QueueDP extends Queue {

    int first;
	String separator = "$query$" ;
	public static boolean cacheResults=false;
    /**
     * Constructor of producer-consumer queue
     *
     * @param address
     * @param cacheResults 
     * @param name
     */
    QueueDP(String address, String input, String output, String cacheResults) {
		super(address, input, output);
		if(cacheResults.equals("true")){
			this.cacheResults=true;
		}
		else{
			this.cacheResults=false;
		}
    }

    /**
     * Add element to the queue.
     *
     * @param i
     * @return
     */

    boolean produce(String query, Watcher w) throws KeeperException, InterruptedException{
    	
        byte[] value;
        value = Bytes.toBytes(query);
        String f =zk.create(input + "/element", value, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
        String filename = "/out/"+f.split("/")[2];
		//System.out.println(filename);
        //zk.exists(filename, w);
        return true;
    }


    /**
     * Remove first element from the queue.
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    int consume(int id) throws KeeperException, InterruptedException{
        Stat stat = null;
        // Get the first element available
        while (true) {
            synchronized (mutex) {
                /*List<String> sync = zk.getChildren("/sync", true);
                if(sync.size()==0){
                    System.out.println("Going to wait:");
                    mutex.wait();
                    //Thread.sleep(500);
                }
                else{*/
                    List<String> list = zk.getChildren(input, true);
                    System.out.println(first+" "+list.size());
                    if(list.size()==0){
                        System.out.println("Going to wait:");
                        mutex.wait();
                        //Thread.sleep(500);
                    }
                    else{
                    	/*Integer min = new Integer(list.get(0).substring(7));

                        String minval=list.get(0).substring(7);
                        for(String s : list){
                            Integer tempValue = new Integer(s.substring(7));
                            //System.out.println("Temporary value: " + tempValue);
                            if(tempValue < min){
                            	min = tempValue;
                            	minval = s.substring(7);
                            }
                        }*/
                    	Random g2 = new Random();
                		int i = g2.nextInt(list.size());
                		String minval=list.get(i).substring(7);
                        String filename=input+"/element" + minval;
                        System.out.println("Temporary file: " + filename);
                        byte[] b = zk.getData(filename,false, stat);
                        zk.delete(filename, 0);
                        if(b[0]!=(byte)0){
                        	byte type = b[0];
                        	byte[] input = new byte[b.length-1];
                        	
                        	for (int j = 0; j < input.length; j++) {
								input[j] = b[j+1];
							}
                        	System.out.println("Api request type: "+type);
    	                    //byte[] outfile = Bytes.toBytes("Hello from server!!");
                        	
                        	ClassLoader parentClassLoader = MyClassLoader.class.getClassLoader();
                        	MyClassLoader classLoader = new MyClassLoader(parentClassLoader);
                        	classLoader.clearAssertionStatus();
                        	String classNameToBeLoaded = "gr.ntua.h2rdf.apiCalls.ApiCalls";
                        	try {
                        		Class myClass = classLoader.loadClass(classNameToBeLoaded);
								
								Object whatInstance = myClass.newInstance();
								Method myMethod = myClass.getMethod("process",
					                    new Class[] { byte.class, byte[].class });
								byte[] outfile = (byte[]) myMethod.invoke(whatInstance,
					                    new Object[] { type, input });
								//byte[] outfile = ExampleApiCall.process(b);
								if(outfile!=null){
									zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
	                                    CreateMode.PERSISTENT);
								}
	    	                    
							} catch (ClassNotFoundException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (InstantiationException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (IllegalAccessException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (NoSuchMethodException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (SecurityException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (IllegalArgumentException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (InvocationTargetException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								System.out.println(e.getCause().toString());
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							}
                        	
    	                    
    	                    return 1;
                        }
                        if(b.length>=2){
    	                    try {
    	                    	long startTimeReal = new Date().getTime();
    	                    	byte[] input = new byte[b.length-1];
    	                    	for (int j = 0; j < input.length; j++) {
    	                    		input[j]=b[j+1];
								}
    	                    	b=input;
	    	                    String data = Bytes.toString(b);
	    	                    String params = data.substring(0,data.indexOf(separator));
	    	                    StringTokenizer tok = new StringTokenizer(params);
	    	                    String table=tok.nextToken("|");
	    	                    //String algo=tok.nextToken("|");
	    	                    //String pool=tok.nextToken("|");
	    	                    //System.out.println("t "+table);
	    	                    String q = data.substring(data.indexOf(separator)+separator.length());
	    	                    //System.out.println(q);
        	                    Query query = QueryFactory.create(q) ;
        	                    

        	        	        // h2rdf new
        	            		Configuration hconf = HBaseConfiguration.create();
        	            		hconf.set("hbase.rpc.timeout", "3600000");
        	            		hconf.set("zookeeper.session.timeout", "3600000");

        	        	        CachingExecutor.connectTable(table, hconf);

        	        	        CachingExecutor executor = new CachingExecutor(table,id);

    	                    	startTimeReal = new Date().getTime();
        	        	        executor.executeQuery(query,true, cacheResults);

        	    	            long stopTime = new Date().getTime();
        	    	            System.out.println("Real time in ms: "+ (stopTime-startTimeReal));
        	                   /* // Generate algebra
        	                    Op opQuery = Algebra.compile(query) ;
        	                    System.out.println(opQuery) ;
        	                    //op = Algebra.optimize(op) ;
        	                    //System.out.println(op) ;
        	                    
        	                    MyOpVisitor v = new MyOpVisitor(minval, query);
        	                    //v.setQuery(query);
        	                    JoinPlaner.setTable(table, "4", "pool45");
        	                    JoinPlaner.setQuery(query);
        	                    OpWalker.walk(opQuery, v);
        	                    v.execute();*/
        	        	        
        	                    //byte[] outfile = Bytes.toBytes(JoinPlaner.getOutputFile());
        	                    byte[] outfile = Bytes.toBytes(executor.visitor.varIds.toString()+"\n"+executor.getOutputFile());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
							} catch (Exception e) {

								e.printStackTrace();
        	                    byte[] outfile = Bytes.toBytes(e.getMessage());
        	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT);
								
								
							}
    	                    return 1;
                        }
                        else{
                        	return 0;
                        }
                    }
                	
            		
                //}
            }
        }
    }
}
