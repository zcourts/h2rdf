package partialJoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.JenkinsHash;

import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MD5Hash;


public class QueryProcessor {
	private static String s;
	private static String p;
	private static String o;
	private static Boolean isVariableS;
	private static Boolean isVariableP;
	private static Boolean isVariableO;
	private static HBaseConfiguration hconf=new HBaseConfiguration();
	private static HTable table;
	private static Object[] vars;
	private static int[] varsNo;
    private static Hash h = JenkinsHash.getInstance();
	
	public static int executeSelect(String bgp, FSDataOutputStream out, String bgpNo) {
		try {
			table = new HTable(hconf, "new");
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(bgp);
		processBgp(bgp);
		return executeSpecifSelect(bgp, out, numVars(), bgpNo);	
	}

	public static Scan getScan(String bgp) {
		System.out.println(bgp);
		processBgp(bgp);
		return getSpecifScan(bgp, numVars());	
	}
	
	private static Scan getSpecifScan(String bgp, int numVars) {
		Scan scan=null;
		String t = getTable();
		byte p=(byte)0;
		if(t.equals("spo"))
			p=(byte)4;
		else if(t.equals("pos"))
			p=(byte)3;
		else if(t.equals("osp"))
			p=(byte)2;
		byte[] row=null;
		byte[] col=null;
		if(numVars==0){
			
		}
		else if(numVars==1){
			if(JoinPlaner.filters.containsKey(getVariable(t.charAt(2))) ){
				System.out.println(getVariable(t.charAt(2)));
				System.out.println(JoinPlaner.filters.get(getVariable(t.charAt(2))));
			}
			scan = new Scan();
			row= new byte[1+8+8];
			row[0]=p;
			byte[] bid=getRowIdbyte(t);
			for (int i = 0; i < 8; i++) {
				row[i+1]=bid[i];
			}
			bid=getColId(t);
			//bid=Bytes.toBytes(colidt);
			//System.out.println(colidt+"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
			for (int i = 0; i < 8; i++) {
				row[i+9]=bid[i];
			}
			System.out.println(Bytes.toStringBinary(row));
			scan.setStartRow(row);
		}
		else if(numVars==2){
			scan = new Scan();
			row= new byte[1+8];
			row[0]=p;
			byte[] bid=getRowIdbyte(t);
			for (int i = 0; i < 8; i++) {
				row[i+1]=bid[i];
			}
			System.out.println(Bytes.toStringBinary(row));
			scan.setStartRow(row);
			byte[] a = Bytes.toBytes(getVariable(t.charAt(1))+"|"+getVariable(t.charAt(2)));
			col = new byte[a.length];
			for (int i = 0; i < a.length; i++) {
				col[i]=a[i];
			}
			System.out.println(Bytes.toStringBinary(col));
			scan.addColumn(col);
			/*scan.setStartRow(Bytes.toBytes(t.charAt(0)+"_"+getRowId(t)+"_"));
			scan.setStopRow(Bytes.toBytes(t.charAt(0)+"_"+getRowId(t)+"_a"));
			scan.addColumn(Bytes.toBytes("A"));*/
		}
		else if(numVars==3){
			
		}
		return scan; 
	}

	private static byte[] getColIdbyte(String table) {
		String string =null;
		if(table.charAt(1)=='s' ){
			string="<"+s+">";
		}
		else if(table.charAt(1)=='p' ){
			string="<"+p+">";
		}
		else if(table.charAt(1)=='o' ){
			if(o.contains("\"")){
				StringTokenizer tok = new StringTokenizer(o);
				String o1= tok.nextToken("^^")+"^^<"+tok.nextToken();
				o1=o1+">";
				o=o1;
				//o=o.replace('"', '<');
			}
			else
				o="<"+o+">";
			string=o;
			System.out.println(o);
		}

		byte[] bst=Bytes.toBytes(string);
		Integer hashVal = Math.abs(h.hash(bst, bst.length, 0));
		byte[] ret = Bytes.toBytes(hashVal);
		//String ret = MD5Hash.digest(string).toString();
		if (ret.length==4)
			return ret;
		else 
			return null;
	}

	private static byte[] getRowIdbyte(String table) {
		String string =null;
		if(table.startsWith("s")){
			System.out.println(s+"ssssssssssssssssssssss");
			string="<"+s+">";
		}
		else if(table.startsWith("p")){
			System.out.println(p+"pppppppppppppppppppppp");
			if(p.equals("rdf:type")){
				string="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
				
			}
			else{
				string="<"+p+">";
			}
		}
		else if(table.startsWith("o")){
			System.out.println(o+"ooooooooooooooooooooooo");
			string="<"+o+">";
		}
		
		MD5Hash md5h = MD5Hash.digest(string);
		long hashVal = Math.abs(md5h.halfDigest());
		
		//Jenkins hash
		// byte[] bst=Bytes.toBytes(string);
		//Integer hashVal = Math.abs(h.hash(bst, bst.length, 0));
		
		byte[] ret = Bytes.toBytes(hashVal);
		if (ret.length==8)
			return ret;
		else 
			return null;
		
	}

	public static String getInpVars(String bgp) {
		int numVars =numVars();
		String ret ="";
		if(numVars==0){
			
		}
		else if(numVars==1){
			String t = getTable();
        	String var1=getVariable(t.charAt(2));
        	ret+=var1;
		}
		else if(numVars==2){
			String t = getTable();
        	String var1=getVariable(t.charAt(1));
        	String var2=getVariable(t.charAt(2));
        	ret+=var1+" ";
        	ret+=var2;
		}
		else if(numVars==3){
			
		}
		return ret; 
	}
	
	private static int numVars() {
		int ret=0;
		if(isVariableS)
			ret++;
		if(isVariableP)
			ret++;
		if(isVariableO)
			ret++;
		return ret;
	}

	private static int executeSpecifSelect( String bgp, FSDataOutputStream out, int numVars, String bgpNo) {
		int ret=0;
		if(numVars==0){
			
		}
		else if(numVars==1){
			try {
				String t = getTable();
				
				Scan scan = new Scan();
				scan.setStartRow(Bytes.toBytes(t.charAt(0)+"_"+getRowId(t)+"_"));
				scan.setStopRow(Bytes.toBytes(t.charAt(0)+"_"+getRowId(t)+"_a"));
				scan.addColumn(Bytes.toBytes("A:"+getColId(t)));
				Iterator<Result> sc = table.getScanner(scan).iterator();
				while(sc.hasNext()){
					Result result = sc.next();
			        ret = result.size();
			        out.writeBytes(bgpNo+" ");
	            	String var1=getVariable(t.charAt(2));
	            	out.writeBytes(String.format("%s$$%s ", var1, Bytes.toString(result.raw()[0].getValue())));
	            	out.writeBytes("\n");
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(numVars==2){
			try {
				String t = getTable();
				//System.out.println(getRowId(t)+"ooooooooooooooooooooooo");
				
				Scan scan = new Scan();
				scan.setStartRow(Bytes.toBytes(t.charAt(0)+"_"+getRowId(t)+"_"));
				scan.setStopRow(Bytes.toBytes(t.charAt(0)+"_"+getRowId(t)+"_a"));
				scan.addColumn(Bytes.toBytes("A"));
				Iterator<Result> sc = table.getScanner(scan).iterator();
				while(sc.hasNext()){
					Result result = sc.next();
			        ret = result.size();
			        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
					for( Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyEntry : map.entrySet()){
						NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = columnFamilyEntry.getValue();
			            for( Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()){
			            	NavigableMap<Long, byte[]> cellMap = columnEntry.getValue();
			                for ( Entry<Long, byte[]> cellEntry : cellMap.entrySet()){
			                	out.writeBytes(bgpNo+" ");
			                	String var1=getVariable(t.charAt(1));
			                	String var2=getVariable(t.charAt(2));
			                	out.writeBytes(String.format("%s$$%s ",var1, Bytes.toString(columnEntry.getKey())));
			                	out.writeBytes(String.format("%s$$%s ", var2, Bytes.toString(cellEntry.getValue())));
			                	out.writeBytes("\n");
			                }

			            }
			        }
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(numVars==3){
			
		}
		
		//return ret; gia arithmo grammwn
		return out.size(); //gia bytes
	}

	private static int getPosition(String s, Object[] st) {
		int j=-1;
    	for (int i = 0; i < st.length; i++) {
			if(s.equals(st[i].toString())){
				j=i;
				break;
			}
		}
		return j;
	}

	private static String getVariable(char c) {
		if(c=='s')
			return s;
		else if(c=='p')
			return p;
		else if(c=='o')
			return o;
		else
			return null;
	}

	private static String getRowId(String table) {
		String string =null;
		if(table.startsWith("s")){
			System.out.println(s+"ssssssssssssssssssssss");
			string="<"+s+">";
		}
		else if(table.startsWith("p")){
			System.out.println(p+"pppppppppppppppppppppp");
			if(p.equals("rdf:type")){
				string="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
				
			}
			else if(p.equals("rdfs:subClassOf")){
				string="<http://www.w3.org/2000/01/rdf-schema#subClassOf>";
				
			}
			else{
				string="<"+p+">";
			}
		}
		else if(table.startsWith("o")){
			System.out.println(o+"ooooooooooooooooooooooo");
			string="<"+o+">";
		}

		byte[] bst=Bytes.toBytes(string);
		Integer hashVal = Math.abs(h.hash(bst, bst.length, 0));
		String ret = hashVal.toString();
		//String ret = MD5Hash.digest(string).toString();
		
		String si1 =null;
		if(ret.startsWith("-")){
			ret = ret.substring(1);
			si1 ="-";
			for (int j = 0; j < 10-ret.length(); j++) {
				si1+="0";
			}
		}
		else{
			si1 ="";
			for (int j = 0; j < 10-ret.length(); j++) {
				si1+="0";
			}
		}
		ret = si1+ret;
		return ret;
	}
	
	private static byte[] getColId(String table) {
		String string =null;
		if(table.charAt(1)=='s' ){
			string="<"+s+">";
		}
		else if(table.charAt(1)=='p' ){
			string="<"+p+">";
		}
		else if(table.charAt(1)=='o' ){
			if(o.contains("\"")){
				/*StringTokenizer tok = new StringTokenizer(o);
				String o1= tok.nextToken("^^")+"^^<"+tok.nextToken();
				o1=o1+">";
				o=o1;*/
				//o=o.replace('"', '<');
			}
			else
				o="<"+o+">";
			string=o;
			System.out.println(o);
		}
		
		//byte[] bst=Bytes.toBytes(string);
		//Integer hashVal = Math.abs(h.hash(bst, bst.length, 0));

		MD5Hash md5h = MD5Hash.digest(string);
		long hashVal = Math.abs(md5h.halfDigest());
		byte[] ret = Bytes.toBytes(hashVal);
		if (ret.length==8)
			return ret;
		else 
			return null;
		
	}
	
	private static String getTable() {
		if(!isVariableS && !isVariableP && !isVariableO)
			return "spo";
		else if(isVariableS && !isVariableP && !isVariableO)
			return "pos";
		else if(!isVariableS && isVariableP && !isVariableO)
			return "osp";
		else if(!isVariableS && !isVariableP && isVariableO)
			return "spo";
		else if(isVariableS && isVariableP && !isVariableO)
			return "osp";
		else if(!isVariableS && isVariableP && isVariableO)
			return "spo";
		else if(isVariableS && !isVariableP && isVariableO)
			return "pos";
		else
			return "spo";
	}
	
	private static void processBgp(String bgp) {
		StringTokenizer tokenizer = new StringTokenizer(bgp);
		s=tokenizer.nextToken();
		if(s.startsWith("?")){
			isVariableS = true;
		}
		else{
			isVariableS = false;
			//s=s.split(":")[1];
		}
		p=tokenizer.nextToken();
		p=p.split("@")[1];
		if(p.startsWith("?")){
			isVariableP = true;
		}
		else{
			isVariableP = false;
			//p=p.split(":")[1];
		}
		o=tokenizer.nextToken();
		if(o.startsWith("?")){
			isVariableO = true;
		}
		else{
			isVariableO = false;
			//o=o.split(":")[1];
		}
			
		if (tokenizer.hasMoreTokens()){
			System.out.println("wrong bgp");
		}
	}

	

	public static void executeJoin(Path outFile, Object[] join_files) {
		int m_rc = 0;
		String[] args=new String[join_files.length+1];
    	args[0]=outFile.getName();
		for (int i = 0; i < join_files.length; i++) {
			args[i+1]=join_files[i].toString().split(":")[0];
		}
        try {
			m_rc = ToolRunner.run(new Configuration(),new HbaseJoinBGP(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void setVars(String bgp) {
		processBgp(bgp);
		for (int i = 0; i < vars.length; i++) {
			if(s.equals(vars[i].toString()))
				varsNo[i]++;
			if(p.equals(vars[i].toString()))
				varsNo[i]++;
			if(o.equals(vars[i].toString()))
				varsNo[i]++;
				
		}
	}

	public static void newVaRS(Object[] v) {
		vars = v;
		varsNo = new int[vars.length];
		for (int i = 0; i < vars.length; i++) {
			varsNo[i]=0;
		}
	}

	public static void printVars() {
		for (int i = 0; i < vars.length; i++) {
			System.out.println(varsNo[i]);
		}
	}


}
