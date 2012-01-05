package byte_import;

import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyNewTotalOrderPartitioner<VALUE> extends Partitioner<ImmutableBytesWritable, VALUE>{
	//private static Random g1= new Random();
	
	public static final int MAX_HBASE_ROWS = 45;
	public static final int MAX_HBASE_BUCKETS = 254;
	private static final byte[] SUBCLASS = Bytes.toBytes( new Long("8742859611446415633"));
	
	@Override
	public int getPartition(ImmutableBytesWritable key, VALUE value,
			int numPartitions) {
		
		
		int partition=0;
		/*StringTokenizer tokenizer = new StringTokenizer(key.toString());
		String s1=tokenizer.nextToken();*/
		byte[] k = key.get();
		byte[] k1 = new byte[8];
		for (int i = 0; i < 8; i++) {
			k1[i]=k[i+1];
		}
		byte pin = (byte) k[0];
		char[] kch = null;//Bytes.toChars(k1);
		char[] s1 = new char[2];
    	for (int i = 0; i < 2 ; i++) {
			s1[i]=kch[i];
		}
		int id = 1;//Bytes.toInt(Bytes.toBytes(s1));
		int def_prt= MAX_HBASE_ROWS;
		int names_prt = 4*MAX_HBASE_ROWS;
		
		int p=0;
		
		if(pin==(byte) 1){
			p=0;
			int first = id/10000000;
			float d= (float)215/(float)names_prt;
			partition = (int) Math.floor(first/d) +p;
			//System.out.println(" partition:"+partition+" first:"+first+" d:"+d);
		}
		else if(pin==(byte) 2){//osp
			Random g2 = new Random();//g1.nextInt() );
			int i = g2.nextInt(MAX_HBASE_ROWS);
			int i2 = g2.nextInt(MAX_HBASE_BUCKETS);
			byte[] k2 = new byte[k.length+2];
			k2[0]=k[0];
			k2[1]=(byte)i;
			for (int ii = 0; ii < 8+8; ii++) {
				k2[ii+2]=k[ii+1];
			}
			k2[18]=(byte)i2;
			for (int ii = 0; ii < 8; ii++) {
				k2[ii+19]=k[ii+17];
			}
			
			key.set(k2,0,k2.length);
			

			p=names_prt;
			partition = i + p;
		}
		else if(pin==(byte) 3){//pos
			Random g2 = new Random();// g1.nextInt() );
			//Check if property is subclass
			int isSubclass=0;
			int i,i2;
			for (int ii = 0; ii < 8; ii++) {
				if(k[ii+1]==SUBCLASS[ii])
					isSubclass++;
			}
			i = g2.nextInt(MAX_HBASE_ROWS);
			i2 = g2.nextInt(MAX_HBASE_BUCKETS);
			if(isSubclass==8){//is subclass
				i = 0;
				i2 = 0;
			}
			else{//no subclass
				i = g2.nextInt(MAX_HBASE_ROWS);
				i2 = g2.nextInt(MAX_HBASE_BUCKETS);
			}
			byte[] k2 = new byte[k.length+2];
			k2[0]=k[0];
			k2[1]=(byte)i;
			for (int ii = 0; ii < 8+8; ii++) {
				k2[ii+2]=k[ii+1];
			}
			k2[18]=(byte)i2;
			for (int ii = 0; ii < 8; ii++) {
				k2[ii+19]=k[ii+17];
			}
			
			key.set(k2,0,k2.length);
			

			p=names_prt+def_prt;
			partition = i + p;
		}
		else if(pin==(byte) 4){//spo
			Random g2 = new Random();// g1.nextInt() );
			int i = g2.nextInt(MAX_HBASE_ROWS);
			int i2 = g2.nextInt(MAX_HBASE_BUCKETS);
			byte[] k2 = new byte[k.length+2];
			k2[0]=k[0];
			k2[1]=(byte)i;
			for (int ii = 0; ii < 8+8; ii++) {
				k2[ii+2]=k[ii+1];
			}
			k2[18]=(byte)i2;
			for (int ii = 0; ii < 8; ii++) {
				k2[ii+19]=k[ii+17];
			}

			key.set(k2,0,k2.length);

			p=names_prt + 2*def_prt;
			partition = i + p;
		}
		return partition;
	}

}
