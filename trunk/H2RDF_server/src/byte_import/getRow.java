package byte_import;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.JenkinsHash;

public class getRow {

	/**
	 * @param args
	 */
    private static Hash h = JenkinsHash.getInstance();
	private static HBaseConfiguration hconf=new HBaseConfiguration();
    
	public static void main(String[] args) {
		//getHash("");
		byte[] row = getRowIdbyte("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
		//byte[] row = getRowIdbyte("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
		/*getRowIdbyte("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#VisitingProfessor>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Dean>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>");*/
		byte[] col=getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>");
		byte[] r = new byte[19];
		r[0]=(byte) 3;
		for (int i = 0; i < 8; i++) {
			r[i+1]=row[i];
		}
		for (int i = 0; i < 8; i++) {
			r[i+9]=col[i];
		}
		try {
			HTable table=null;
			table = new HTable( hconf, "new2" );
			/*for (int i = 0; i < 35; i++) {
				r[17]=(byte) i;
				Get get = new Get(r);
				table = new HTable( hconf, "new2" );
				Result res = table.get(get);
				System.out.println("row: "+Bytes.toStringBinary(r)+" size: "+res.size());
				//System.out.println("starttttttttttttttttttttttttt");
				//System.out.println(res.size());
				//int j=0;
				//for (int j2 = 0; j2 < l.length; j2++) {
				//	System.out.println(i+" "+valueToString(result.getCellValue().getValue()));
				//	i++;
				//}
				
			}*/
			r[17]=(byte) 255;
			r[18]=(byte) 255;
			Get get = new Get(r);
			Result res = table.get(get);
			if(res.size()>0)
				System.out.println("row: "+Bytes.toStringBinary(r)+" size: "+Bytes.toLong(res.raw()[0].getQualifier()));
			System.out.println(table.getRegionLocation(r).getRegionInfo().getRegionId());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		

	}
	

	private static byte[] getRowIdbyte(String string) {
		//Integer hashVal = Integer.parseInt(string);
		MD5Hash j = MD5Hash.digest(string);
		long hashVal= Math.abs(j.halfDigest());
		//byte[] bst=Bytes.toBytes(string);
		//Integer hashVal = Math.abs(h.hash(bst, bst.length, 0));
		System.out.println("string: "+string+" hashval: "+hashVal);
		byte[] ret = Bytes.toBytes(hashVal);
		System.out.println("string: "+string+" hashval: "+hashVal+" bytes: "+Bytes.toStringBinary(ret));
		//String ret = MD5Hash.digest(string).toString();
		if (ret.length==8)
			return ret;
		else 
			return null;
		
	}
	
	private static void getHash(String string) {
		char[] ch= string.toCharArray();
		String num = "", str = "";
		for (int i = 0; i < ch.length; i++) {
			if(ch[i]>='0' && ch[i]<='9'){
				num+=ch[i];
			}
			else{
				str+=ch[i];
			}
		}
		byte[] nu=Bytes.toBytes(num);
		Integer hashVal1 = Math.abs(h.hash(nu, nu.length, 0));
		byte[] st=Bytes.toBytes(str);
		Integer hashVal2 = Math.abs(h.hash(st, st.length, 0));
		System.out.println(hashVal1+" "+hashVal2);
		//Integer sunolo = hashVal
		System.out.println(str);
		System.out.println(num);
		
	}
}
