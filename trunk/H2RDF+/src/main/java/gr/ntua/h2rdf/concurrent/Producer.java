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

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
public class Producer  implements Watcher{
	private final String NL;
    static int finished, total, prevOut=0;
    static boolean dead;
    private Queue q;
    private FileSystem fs;
	private ZooKeeper zk;
	private static String separator = "$query$" ;
	long startTime;
	
	public Producer(String address, Queue q, ZooKeeper zk) {
		Configuration conf = new Configuration();
    	
        try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        this.q = q;
        this.zk=zk;
        NL = System.getProperty("line.separator") ;
	}

	public void produce(String[] args) {
		dead=false;

        if (args[2].equals("p")) {
            Integer maxUniv = new Integer(args[3]);
            Integer maxDep = new Integer(args[4]);
            Integer maxCourse = new Integer(args[5]);
            Integer qid = new Integer(args[6]);
            String table = args[7];
            Integer averTh = new Integer(args[8]);
            Integer maxTh = new Integer(args[9]);
            Integer period = new Integer(args[10]);
            
            System.out.println("Producer");
            String prolog = "PREFIX dc: <http://dbpedia.org/resource/>"+
			"PREFIX p: <http://dbpedia.org/ontology/>"+
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
			"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>";
            
			String q1 = table+ separator+prolog + NL +
			"SELECT  ?x " +
			"WHERE  { ?x rdf:type ub:GraduateStudent ." +
			"?x ub:takesCourse <http://www.Department";
			
			String q3_1 = table+ separator+prolog + NL +
					"SELECT  ?x ?1 " +
					"WHERE   { ?x rdf:type ub:Publication ." +
					"?x ub:publicationAuthor <http://www.Department";
			
			String q4_1 = table+ separator+prolog + NL +
					"SELECT  ?x ?n ?em ?t " +
					"WHERE   { ?x ub:worksFor <http://www.Department" ;
			String q4_2 =	"?x rdf:type ub:FullProfessor ."+
					"?x ub:name ?n ." +
					"?x ub:emailAddress ?em ." +
					"?x ub:telephone ?t " +
					"}";	
			
			finished=0;
			total=0;
			String filename="";
			startTime= System.currentTimeMillis();
			long sleep=0, d=0, tmpd, outstart;
			int nanos=0;
			double throughput=0, delay = 15.5;
			try {
				prevOut=zk.getChildren("/out", false).size();
			} catch (KeeperException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			outstart=startTime;
			int max=maxUniv*maxDep*maxCourse;

        	//new ThreadedH2RDFClientStats(zk, fs).start();
			while(true){
            for (int i = 11000; i < 11000+maxUniv; i++){
            	long st =System.currentTimeMillis();
            	for (int j = 0; j < maxDep; j++){
            		for (int k = 0; k < maxCourse; k++){
		                try{
		                	tmpd =System.currentTimeMillis();
		                	if(qid==1){
			                	String q2=q1+j+".University"+i+".edu/GraduateCourse"+k+">}";
			                	//System.out.println(q2);
			                	new ThreadedH2RDFClient(q,q2, this).start();
			                    //q.produce(q2, this);
		                	}
		                	else if(qid==3){
			                	String q3=q3_1+j+".University"+i+".edu/AssistantProfessor"+k+">}";
			                	//System.out.println(q3);
			                	new ThreadedH2RDFClient(q,q3, this).start();
			                    //q.produce(q3, this);
		                	}
		                	else if(qid==4){
			                	String q4= q4_1+j+".University"+(i+k*10)+".edu> . "+q4_2;
			                	//System.out.println(q4);
			                	new ThreadedH2RDFClient(q,q4, this).start();
			                    //q.produce(q4, this);
		                	}
		                	//System.out.println(delay);
		                    total++;
		                    //System.out.println(total);
		                   /* if(total == max){
		                    	//byte[] value= new byte[0];
		                        //filename =zk.create("/sync" + "/e", value, Ids.OPEN_ACL_UNSAFE,
		                        //            CreateMode.PERSISTENT_SEQUENTIAL);
		                    	startTime = new Date().getTime();
		                    }*/
		                    long time = System.currentTimeMillis()-startTime;
		                    throughput = averTh+maxTh*Math.sin(time*2*Math.PI/period);
		                    //throughput = averTh;
		                    //delay=14.5+throughput*0.2;
		                    //if(delay>=19)
		                    //	delay=19;
		                    if(period==1)
		                    	throughput = averTh;
		                	tmpd=System.currentTimeMillis()-tmpd;
		                	d+=tmpd;
		                	delay=0;
		                	//if(throughput*delay<1000){
			                    sleep = Math.round(Math.floor((1000-throughput*delay)/throughput));
			                    nanos = (int) (((1000-throughput*delay)/throughput-sleep)*1000);
			                    //System.out.println("time: "+time+" throughput: "+throughput+" sleep: "+sleep);
			                    //sleep =0 ;
			                    if(sleep>0)
			                    	Thread.sleep(sleep,nanos);
		                	//}
		                    	
		                //} catch (KeeperException e){
		
		                } catch (InterruptedException e){
		
		                }
            		}
                	delay =(double)d/((double)maxCourse);
                	if(delay<11)
                		delay=11;
                	d=0;
            	}

            	try {
	            	long st1 =System.currentTimeMillis();
	            	double t = (double)maxDep*maxCourse*1000/(double)(st1-st);
	            	System.out.println(" throughput: "+throughput+" sleep: "+sleep+" "+nanos+" delay: "+delay);
	            	
	            	String[] ganglia_throughput_command = new String[]{"gmetric", "-n",
	            			"in_THROUGHPUT", "-v", Double.toString(throughput), "-d","600" ,"-t",
	            			"double" , "-u",  "qps"};
	            	String[] ganglia_real_throughput_command = new String[]{"gmetric", "-n",
            			"in_real_THROUGHPUT", "-v", Double.toString(t), "-d","600" ,"-t",
            			"double" , "-u",  "qps"};
	    			Process child = Runtime.getRuntime().exec(ganglia_throughput_command);
	    			child = Runtime.getRuntime().exec(ganglia_real_throughput_command);
	
	            	System.out.println("Real throughput: "+t);
	            	/*if(i%4 ==0){
		            	st1 =System.currentTimeMillis();
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
		        		ganglia_real_throughput_command = new String[]{"gmetric", "-n",
		            			"out_real_THROUGHPUT", "-v", Double.toString(out_t), "-d","600" ,"-t",
		            			"double" , "-u",  "qps"};
			    		child = Runtime.getRuntime().exec(ganglia_real_throughput_command);

	            		int qlen=zk.getChildren("/in", false).size();
		        		System.out.println("Real output throughput: "+out_t+" queue: "+qlen);
		        		ganglia_real_throughput_command = new String[]{"gmetric", "-n",
		            			"qlen", "-v",""+qlen, "-d","600" ,"-t",
		            			"double" , "-u",  "queries"};
			    		child = Runtime.getRuntime().exec(ganglia_real_throughput_command);
		        		outstart=System.currentTimeMillis();
	            	}*/
	    		} catch (IOException e) {
	    			// TODO Auto-generated catch block
	    			e.printStackTrace();
    			}
            }
	        }
            //long stop = System.currentTimeMillis();
			//double t = (double) (stop-startTime)/(double) max;
            //System.out.println("max: "+max+" delay: "+ t);
           /* try {
                synchronized (this) {
                    while (!dead) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
            	try {
					zk.delete(filename, 0);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (KeeperException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                long stopTime = new Date().getTime();
                System.out.println("Real time in ms: "+ (stopTime-startTime));
            }*/
            
            /*try {
				zk.delete(filename, 0);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
            //long stopTime = new Date().getTime();
            //System.out.println("Real time in ms: "+ (stopTime-startTime));
        }
	}

	@Override
	public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
        }
        else{
	        if(path.startsWith("/out") && (event.getType() == Event.EventType.NodeCreated)){
	            // It's all over
	        	try {
	        		int temp = zk.getChildren("/out", true).size();
	        		if (temp-prevOut>=400){
	        			long stop = new Date().getTime();
	        			double t = (double) (temp-prevOut)*1000/(double)(stop-startTime);
	        			System.out.println("start: "+ startTime+" stop: "+stop+" throughput: "+t);
	        			startTime = new Date().getTime();
	        			prevOut=temp;
	        			//finished=0;
	        		}
	        		/*finished++;
	        		if(finished>=400){
	        			long stop = new Date().getTime();
	        			double t = (double) 400*1000/(double)(stop-startTime);
	        			System.out.println("start: "+ startTime+" stop: "+stop+" throughput: "+t);
	        			startTime = new Date().getTime();
	        			finished=0;
	        		}*/
	        		
		            //zk.delete(path, 0);
	        		//fs.delete("output/"+Join, arg1)
	        		
	        		if(finished>=total){
	        			System.out.println("Number of Queries: "+total);
			        	dead=true;
			            synchronized (this) {
			                notifyAll();
			            }
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
	

}
