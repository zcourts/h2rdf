package concurrent;

import java.io.IOException;
import java.util.Date;

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
import org.apache.zookeeper.ZooDefs.Ids;

import partialJoin.JoinPlaner;

public class Producer  implements Watcher{
	private final String NL;
    static int finished, total;
    static boolean dead;
    private Queue q;
    private FileSystem fs;
	private ZooKeeper zk;
	
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
            
            System.out.println("Producer");
            String prolog = "PREFIX dc: <http://dbpedia.org/resource/>"+
			"PREFIX p: <http://dbpedia.org/ontology/>"+
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
			"PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>"+
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>";
            
			String q1 = "new2"+ NL+prolog + NL +
			"SELECT  ?x " +
			"WHERE  { ?x rdf:type ub:GraduateStudent ." +
			"?x ub:takesCourse <http://www.Department";
			
			String q3_1 = prolog + NL +
					"SELECT  ?x ?1 " +
					"WHERE   { ?x rdf:type ub:Publication ." +
					"?x ub:publicationAuthor <http://www.Department";
			
			String q4_1 = prolog + NL +
					"SELECT  ?x ?n ?em ?t " +
					"WHERE   { ?x ub:worksFor <http://www.Department" ;
			String q4_2 =	"?x rdf:type ub:Professor ."+
					"?x ub:name ?n ." +
					"?x ub:emailAddress ?em ." +
					"?x ub:telephone ?t " +
					"}";	
			
			finished=0;
			total=0;
			String filename="";
			long startTime= new Long(0);
			int max=maxUniv*maxDep*maxCourse;
            for (int i = 11000; i < 11000+maxUniv; i++){
            	for (int j = 0; j < maxDep; j++){
            		for (int k = 0; k < maxCourse; k++){
		                try{
		                	if(qid==1){
			                	String q2=q1+j+".University"+i+".edu/GraduateCourse"+k+">}";
			                	System.out.println(q2);
			                    q.produce(q2, this);
		                	}
		                	else if(qid==3){
			                	String q3=q3_1+j+".University"+i+".edu/AssistantProfessor"+k+">}";
			                	System.out.println(q3);
			                    q.produce(q3, this);
		                	}
		                	else if(qid==4){
			                	String q4= q4_1+j+".University"+i+".edu> . "+q4_2;
			                	System.out.println(q4);
			                    q.produce(q4, this);
		                	}
		                    total++;
		                    if(total == max){
		                    	byte[] value= new byte[0];
		                        filename =zk.create("/sync" + "/e", value, Ids.OPEN_ACL_UNSAFE,
		                                    CreateMode.PERSISTENT_SEQUENTIAL);
		                    	startTime = new Date().getTime();
		                    }
		                    	
		                } catch (KeeperException e){
		
		                } catch (InterruptedException e){
		
		                }
            		}
            	}
            }
            try {
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
            }
            try {
				zk.delete(filename, 0);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            long stopTime = new Date().getTime();
            System.out.println("Real time in ms: "+ (stopTime-startTime));
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
	        		finished++;
		            zk.delete(path, 0);
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
