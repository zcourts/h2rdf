package concurrent;

import java.nio.ByteBuffer;

import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import partialJoin.JoinPlaner;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.*;

public class Queue extends SyncPrimitive {

    int first;
	String NL = System.getProperty("line.separator") ;
    /**
     * Constructor of producer-consumer queue
     *
     * @param address
     * @param name
     */
    Queue(String address, String input, String output) {
        super(address);
        first=0;
        this.input = input;
        this.output = output;
        // Create ZK node name
        if (zk != null) {
            try {
                Stat s = zk.exists(input, false);
                if (s == null) {
                    zk.create(input, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
                s = zk.exists(output, false);
                if (s == null) {
                    zk.create(output, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
                s = zk.exists("/sync", false);
                if (s == null) {
                    zk.create("/sync", new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                System.out
                        .println("Keeper exception when instantiating queue: "
                                + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }
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
		System.out.println(filename);
        zk.exists(filename, w);
        return true;
    }


    /**
     * Remove first element from the queue.
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    int consume() throws KeeperException, InterruptedException{
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
                        if(b.length>=2){
    	                    String data = Bytes.toString(b);
    	                    String table = data.split(NL)[0];
    	                    String q = data.substring(data.indexOf(NL));
    	                    //System.out.println(q);
    	                    Query query = QueryFactory.create(q) ;
    	                    // Generate algebra
    	                    Op opQuery = Algebra.compile(query) ;
    	                    System.out.println(opQuery) ;
    	                    //op = Algebra.optimize(op) ;
    	                    //System.out.println(op) ;
    	                    
    	                    MyOpVisitor v = new MyOpVisitor(minval, query);
    	                    //v.setQuery(query);
    	                    JoinPlaner.setTable(table);
    	                    JoinPlaner.setQuery(query);
    	                    OpWalker.walk(opQuery, v);
    	                    byte[] outfile = Bytes.toBytes(JoinPlaner.getOutputFile());
    	                    zk.create(output + "/element" + minval, outfile,Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT);
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
