package partialJoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import translator.Translator;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;

public class JoinPlaner {
	private static Hashtable<String, Integer> hvars, hvars_temp;
	private static String[] Q;
	private static String[] Q_files, join_files, join_files_vars, inpVars;
	private static Scan[] ScanBgp;
	private static String[][] Q_files2;
	private static List<Ecount> varsEcount;
	private static String[] vars;
	public static Integer joins;
	private static float fulljoincost;
	private static String joinpat, tableName;
	private static FileSystem fs;
	private static Query query;
	private static byte[] SUBCLASS = Bytes.toBytes( new Long("8742859611446415633"));
	private static Configuration hconf;
	public static String id="";
	private static HTable table =null;
	public static HashMap<String,List<ExprFunction>> filters;
	
	public static void setQuery(Query q){
		query = q;
	}
	
	public static Scan getScan(int i){
		return ScanBgp[i];
	}
	
	public static String[] form(String[] bgp) {
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Random g2 = new Random();
		//rani = g2.nextInt(Integer.MAX_VALUE);
		joins=0;
    	Q_files= new String[bgp.length];
    	Q_files2= new String[bgp.length][2];
    	ScanBgp = new Scan[bgp.length];
    	inpVars = new String[bgp.length];
		for (int i = 0; i < bgp.length; i++) {
        	Q_files[i]="output/BGP"+i;
        	Q_files2[i][0]="output/BGP"+i;
        	ScanBgp[i]=QueryProcessor.getScan(bgp[i]);
        	inpVars[i]=QueryProcessor.getInpVars(bgp[i]);
        	
        	/*Q_files2[0][1]= "100000";
        	Q_files2[1][1]= "40";*/
        	
        	/*if(Q_files2.length==6 && bgp[1].contains("Professor")){
	        	Q_files2[0][1]= "100000";
	        	Q_files2[1][1]= "5000";
	        	Q_files2[2][1]= "8000";
	        	Q_files2[3][1]= "50000";
	        	Q_files2[4][1]= "300000";
	        	Q_files2[5][1]= "8000";
        	}
        	else if(Q_files2.length==6){
	        	Q_files2[0][1]= "100000";
	        	Q_files2[1][1]= "5000";
	        	Q_files2[2][1]= "8000";
	        	Q_files2[3][1]= "80000";
	        	Q_files2[4][1]= "10000";
	        	Q_files2[5][1]= "8000";
        	}
        	else if(Q_files2.length==2){
        		Q_files2[0][1]= "10";
            	Q_files2[1][1]= "40";
        	}
        	else if(Q_files2.length==5){
            	Q_files2[0][1]= "50";
            	Q_files2[1][1]= "800000";
            	Q_files2[2][1]= "800000";
            	Q_files2[3][1]= "800000";
            	Q_files2[4][1]= "800000";
        	}
        	else if(Q_files2.length==4){
        		Q_files2[0][1]= "800000";
        		Q_files2[1][1]= "800000";
        		Q_files2[2][1]= "4";
        		Q_files2[3][1]= "8000000";
        	}*/
        	
        	Q_files2[i][1]=getBGPSize(ScanBgp[i]);
        	/*if(bgp[i].toString().contains("?y")){
        		Q_files2[i][1]= "1000";
        	}
        	else if(bgp[i].toString().contains("?x")){
        		Q_files2[i][1]= "10000000";
        	}
        	else if(bgp[i].toString().contains("?z")){
        		Q_files2[i][1]= "500";
        	}*/
		}
        for (int i = 0; i < bgp.length; i++) {
			bgp[i]=form(bgp[i],i);
		}
        
		return bgp;
	}

	private static String getBGPSize(Scan scan) {
		byte[] rowid = scan.getStartRow();
		byte[] r = new byte[1+8+8+2];
		Long size= new Long(0);
		if(rowid.length==1+8){//2 variable
			r[0]=rowid[0];
			for (int i = 1; i < rowid.length; i++) {
				r[i]=rowid[i];
			}
			for (int i = 1+8; i < r.length; i++) {
				r[i]=(byte) 255;
			}
			Get get = new Get(r);
			try {
				Result result  = table.get(get);
				if(result.size()!=0){
					size = Bytes.toLong(result.raw()[0].getValue());
					//KeyValue[] v = result.raw();
					//size = Bytes.toLong(v[0].getQualifier());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if(rowid.length==1+8+8){//1 variable
			
			byte[] objid = new byte[8];
			for (int i = 0; i < 8; i++) {
				objid[i]=rowid[i+9];
			}
			byte[] classrowStart = new byte[1+8+8+2];
			byte[] classrowStop = new byte[1+8+8+2];
			classrowStart[0]=(byte)3; //pos
			for (int i1 = 0; i1 < 8; i1++) {
				classrowStart[i1+1]=SUBCLASS[i1];
			}
			for (int i1 = 0; i1 < 8; i1++) {
				classrowStart[i1+9]=objid[i1];
			}
			for (int i1 = 0;  i1< classrowStart.length-1; i1++) {
				classrowStop[i1]=classrowStart[i1];
			}
			
			classrowStart[classrowStart.length-2] = (byte) 0;
			classrowStart[classrowStart.length-1] = (byte) 0;
			classrowStop[classrowStop.length-2] = (byte) 255;
			classrowStop[classrowStop.length-1] = (byte) 255;
			
			
			byte[] bid,a;
			a=Bytes.toBytes("A");
			bid = new byte[a.length];
			for (int i = 0; i < a.length; i++) {
				bid[i]=a[i];
			}
			Scan scan1 =new Scan();
			scan1.setStartRow(classrowStart);
			scan1.setStopRow(classrowStop);
			scan1.setCaching(254);
			scan1.addFamily(bid);
			try {
				ResultScanner resultScanner = table.getScanner(scan1);
				Result result = null;
				if((result=resultScanner.next())!=null){
					if(result.size()>0){
						size=new Long(100000);
						return size.toString();
					}
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			r[0]=rowid[0];
			for (int i = 1; i < rowid.length; i++) {
				r[i]=rowid[i];
			}
			r[r.length-2]=(byte)255;//size
			r[r.length-1]=(byte)255;//size
			Get get = new Get(r);
			try {
				Result result  = table.get(get);
				if(result.size()!=0){
					size = Bytes.toLong(result.raw()[0].getValue());
					//KeyValue[] v = result.raw();
					//size = Bytes.toLong(v[0].getQualifier());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(size==0){
			return "20";
		}
		else{
			return size.toString();
		}
	}

	
	public static String[] form2(String[] bgp) {
		for (int i = 0; i < bgp.length; i++) {
			bgp[i]=form(bgp[i], i);
		}
        
		return bgp;
	}
	
	public static void newVaRS(String[] v) {
		vars=v;
		hvars = new Hashtable<String, Integer>();
		for (int i = 0; i < v.length; i++) {
			hvars.put(v[i], new Integer(0));
		}
	}
	
	private static String form(String pt, int i) {
		StringTokenizer tokenizer = new StringTokenizer(pt);
		String ret="", s;
		while(tokenizer.hasMoreTokens()){
			s=tokenizer.nextToken();
			if(s.startsWith("?")){
				ret+=s+" ";
				Integer no = hvars.get(s);
				no=no+1;
				hvars.put(s, no);
			}
			if(s.startsWith("@?")){
				s=s.substring(1);
				ret+=s+" ";
				Integer no = hvars.get(s);
				no=no+1;
				hvars.put(s, no);
			}
		}
		return ret;
	}
	
	public static void printVars() {
		System.out.println(hvars);
	}

	public static String[] removeNonJoiningVaribles(String[] bgp) {

		Q=bgp;
		for (int i = 0; i < bgp.length; i++) {
			StringTokenizer tokenizer = new StringTokenizer( bgp[i]);
			String temp="",s;
			while(tokenizer.hasMoreTokens()){
				s=tokenizer.nextToken();
				if( hvars.get(s)>=2){
					temp += s+" ";
				}
			}
			bgp[i]=temp;
		}
		Q=bgp;
		return bgp;
	}

	public static boolean isEmpty() {
		int j=Q.length;
		for (int i = 0; i < Q.length; i++) {
			if(Q[i].equals("")){
				j--;
			}
		}
		return j==0;
	}

	public static void sortByECounts() {
		varsEcount = new ArrayList<Ecount>();
		Enumeration<String> e = hvars.keys();
		while( e. hasMoreElements() ){
			String s=e.nextElement();
			if(hvars.get(s)>=2)
				varsEcount.add(new Ecount(greedy(s), s));
			
		}
		//for (Ecount e1: varsEcount) {
		//	System.out.println(e1.getName()+"uuuuuuuuuu"+Float.toString(e1.getECount()));
		//}
	}

	private static float greedy(String var) {
		float e=0, sel_sum=0, tot_sum=0, non_sel=0;
		int index_access=4;
		float greedy;
		for (int i = 0; i < Q.length; i++) {
			StringTokenizer tokenizer = new StringTokenizer(Q[i]);
			String s;
			int j=0, found=0;
			while(tokenizer.hasMoreTokens()){
				s=tokenizer.nextToken();
				if(s.equals(var)){
					found=1;
					j--;
				}
				j++;
			}
			if(found==1){
				e+=j;
				long size = Long.parseLong(Q_files2[i][1]);
				if (size<=1000) //selective
					sel_sum+=size;
				else //non selective  
					non_sel++;
				tot_sum+=size;
			}
		}
		sel_sum=sel_sum*(non_sel+1)*index_access;
		if(sel_sum>0 && sel_sum<tot_sum){
			greedy = e*1000000 + sel_sum;
		}
		else{
			greedy = e*1000000 + tot_sum;
		}
		return greedy;
	}

	private static long sum() {
		long sum=0;
		for (int i = 0; i < Q.length; i++) {
			sum+=Long.parseLong(Q_files2[i][1]);
		}
		return sum;
	}

	public static String getNextJoin() {
		joinpat="";
    	join_files = new String[0];
    	join_files_vars = new String[0];
    	float joinSize=0,jCost=-1;
    	sortByECounts();
		Collections.sort(varsEcount);
		printQ();
		String ret="", retno="";
		String[] temp= new String[varsEcount.size()];
		int tempi=0;
		hvars_temp=hvars;
		int count=0;
		for (Ecount e: varsEcount) {
			if(canEliminate(e.getName())){
				if(count==0 ){
					jCost = joinCost(e.getName(),true);
				}
				else if(count>=1 ){
					jCost = joinCost(e.getName(),false);
				}
				
				if(jCost>0){
					joinSize+=jCost;
					ret+=e.getName()+" ";
					retno+=TPno(e.getName())+" ";
					temp[tempi] = TP(e.getName());
					tempi++;
					varsEcount.remove(e.getName());
				}
			}
			count++;
			printQ();
		}
		//printJoinFiles();
		Path outFile=new Path("output/Join_"+id+"_"+joins);
		Path vars=new Path("input/JoinVars_"+id+"_"+joins);
		String[] lines=null;
		String[] sfiles=null;
		boolean mapReduceTranslate=false;
		try {
			if (fs.exists(outFile)) {
				fs.delete(outFile,true);
			}

			if (fs.exists(vars)) {
				fs.delete(vars,true);
			}
			FSDataOutputStream v= fs.create(vars);
			lines=printJoinV(v, ret);
			for (int i = 0; i < lines.length; i++) {
				System.out.println(lines[i]);
			}
			sfiles = splitFiles(join_files,lines);
			//v.writeBytes(ret+"\n");
			v.writeBytes(retno+"\n");
			v.writeBytes(joins+"\n");
			insertLines(temp, tempi);
			joins++;
			printQ();
			if(isEmpty()){
				if(joinSize>=70000000){//MapReduce translate
					v.writeBytes("0\n");
					v.writeBytes("\n");
					//mapReduceTranslate=true;
					//v.writeBytes("2\n");
					//v.writeBytes(query.getResultVars().toString().replace(',', ' ').replace('[', ' ').replace(']', ' ').trim()+"\n");
				}
				else{//index translate
					v.writeBytes("1\n");
					v.writeBytes(query.getResultVars().toString().replace(',', ' ').replace('[', ' ').replace(']', ' ').trim()+"\n");
				}
			}
			else{
				v.writeBytes("0\n");
				v.writeBytes("\n");
			}
			printNonJoinV(v, ret, lines);
			
			v.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println(joinSize);
		if(joinSize >= 350000)
			QueryProcessor.executeJoin(outFile, sfiles);
		else
			SerialQueryProcessor.executeJoin(outFile, sfiles);
		updateSize();
		if(mapReduceTranslate)
			Translator.executeTranslate(new Path("output/Join_"+id+"_"+joins), outFile);
		
		try {
			if (fs.exists(vars)) {
				fs.delete(vars,true);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//deleteJoinFiles();
		return ret;
	}
	


	private static float joinCost(String var, Boolean isTotalEllimination) {
		float sel_sum=0, tot_sum=0, non_sel=0;
		int index_access=1;
		String joinInSel="", joinInTot="";
		for (int i = 0; i < Q.length; i++) {
			if(Q[i].contains(var)){
				long size = Long.parseLong(Q_files2[i][1]);
				if (size<=1000){ //selective
					sel_sum+=size;
					if(Q_files[i].contains("B"))
						joinInSel+= Q_files[i].split("BGP")[1];
				}
				else //non selective  
					non_sel++;
				if(Q_files[i].contains("B"))
					joinInTot+=Q_files[i].split("BGP")[1];
				tot_sum+=size;
			}
		}
		sel_sum=sel_sum*(non_sel+1)*index_access;
		if(sel_sum>0 && sel_sum<tot_sum){
			if(isTotalEllimination){
				joinpat+=joinInSel;
				fulljoincost=sel_sum;
				return sel_sum;
			}
			else{
				if(fulljoincost>sel_sum*2){
					joinpat+=joinInSel;
					return sel_sum;
				}
				else{
					return 0;
				}
			}
		}
		else{
			if(isTotalEllimination){
				joinpat+=joinInTot;
				fulljoincost=tot_sum;
				return tot_sum;
			}
			else{
				if(fulljoincost>tot_sum*2){
					joinpat+=joinInTot;
					return tot_sum;
				}
				else{
					return 0;
				}
			}
		}
	}

	private static String[] splitFiles(String[] joinFiles, String[] lines) {
		int count =0;
		for (int i = 0; i < joinFiles.length; i++) {
			if(lines[i].contains("{"))
				count++;
		}
		String[] ret = new String[count];
		count =0;
		for (int i = 0; i < joinFiles.length; i++) {
			if(lines[i].contains("{")){
				ret[count]=lines[i].substring(lines[i].indexOf("{")+1, lines[i].length());
				count++;
			}
		}
		return ret;
	}

	private static void printNonJoinV(FSDataOutputStream v, String ret, String[] lines) {
		try {
			for (int i = 0; i < join_files.length; i++) {
				if(lines[i].contains("|")){
					if(lines[i].contains("J"))
						System.exit(1);
					String fname=lines[i].substring(0, lines[i].indexOf("|"));
					Bytes.writeByteArray(v, Bytes.toBytes(fname));
					int id=Integer.parseInt(String.valueOf(lines[i].charAt(lines[i].length()-1)));
					Bytes.writeByteArray(v, getScan(id).getStartRow());
					Bytes.writeByteArray(v, Bytes.toBytes(getScan(id).getInputColumns()));
				}
			}
			Bytes.writeByteArray(v, Bytes.toBytes("end"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String[] printJoinV(FSDataOutputStream v, String ret) {
		HashSet<String> varSet = new HashSet<String>();
		String[] lines = new String[join_files.length];
		for (int i = 0; i < join_files.length; i++) {
			lines[i]="{";
		}
		//String joinpat = query.getResultVars().get(query.getResultVars().size()-1).toString();
		try {
			v.writeBytes(ret+"\n");
			for (int i = 0; i < join_files.length; i++) {
				String jf = join_files[i];
				//if(!varSet.contains(join_files_vars[i])){
				System.out.println("joinpat: "+joinpat);
				if(jf.contains("BGP") && joinpat.contains(jf.split("BGP")[1])){
					System.out.println(join_files_vars[i]+";;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;");
					varSet.add(join_files_vars[i]);
					String id="";
					if(jf.contains("BGP")){
						id+="P";
						id+= jf.split("BGP")[1];
						lines[i]=join_files_vars[i]+"{"+jf;
					}
					else{
						id+="J";
						id+= jf.split("_")[2];
						lines[i]+=jf;
					}
					v.writeBytes(id+" "+join_files_vars[i]+" $$ ");
				}
				else{
					String id="";
					if(jf.contains("BGP")){
						lines[i]=join_files_vars[i]+"|"+jf;
					}
					else{
						id+="J";
						id+= jf.split("_")[2];
						lines[i]+=jf;
						v.writeBytes(id+" "+join_files_vars[i]+" $$ ");
					}
				}
			}
			v.writeBytes("\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lines;
	}

	private static void deleteJoinFiles() {
		for (int i = 0; i < join_files.length; i++) {
			try {
				fs.delete(new Path(join_files[i]),true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}

	private static String TPno(String name) {
		Integer lines = 0;
		
		for (int i = 0; i < Q.length; i++) {
			StringTokenizer tokenizer = new StringTokenizer( Q[i]);
			String s;
			while(tokenizer.hasMoreTokens()){
				s=tokenizer.nextToken();
				if(s.equals(name)){
					lines++;
				}
			}
		}
		return lines.toString();
	}

	private static void printJoinFiles() {
		for (int i = 0; i < join_files.length; i++) {
			System.out.println(join_files[i]+"   "+join_files_vars[i]);
		}
		
	}

	public static void printQ() {
		for (int i = 0; i < Q.length; i++) {
			System.out.println(Q[i]+"    "+Q_files2[i][0]+"    "+Q_files2[i][1]);
		}
		System.out.println("-------------");
	}

	
	
	private static boolean canEliminate(String name) {
		return hvars.get(name)>=2;
	}
	
	public static String TP(String name) {
		String ret="";
		int[] lines= new int[Q.length];
		
		for (int i = 0; i < Q.length; i++) {
			lines[i]=0;
			StringTokenizer tokenizer = new StringTokenizer( Q[i]);
			String s;
			while(tokenizer.hasMoreTokens()){
				s=tokenizer.nextToken();
				if(s.equals(name)){
					lines[i]=1;
				}
			}
		}
		
		ret = joinResult(lines,name);
		deleteLines(lines, name);
		return ret;
	}

	private static String joinResult(int[] lines, String name) {
		
		int no=0;
		String ret="";
		for (int i = 0; i < Q.length; i++) {
			if(lines[i]==1){
				StringTokenizer tokenizer = new StringTokenizer( Q[i]);
				String s;
				while(tokenizer.hasMoreTokens()){
					s=tokenizer.nextToken();
					if(!s.equals(name)){
						if(!ret.contains(s))
							ret+=s+" ";
					}
				}
				no++;
			}
		}
		//partial elimination
		//System.out.println((Integer) hvars_temp.get(name));
		//System.out.println(no);
		//System.out.println("----------------------------------------------------");
		if(hvars_temp.get(name)>=50){
			if(hvars_temp.get(name)-50>no)
				ret+=name+" ";
		}
		else if(hvars_temp.get(name)>no)
			ret+=name+" ";
		return ret;
	}

	private static void deleteLines(int[] lines, String name) {
		int j=0;

		for (int i = 0; i < Q.length; i++) {
			if(lines[i]==0)
				j++;
		}
		String[] tempQ= new String[j];
		String[] tempQ_files= new String[j];
		String[][] tempQ_files2= new String[j][2];
		int jj=join_files.length;
		String[] t_join_files = new String[Q.length-j+jj];
		String[] t_join_files_vars = new String[Q.length-j+jj];
    	for (int i = 0; i < jj; i++) {
    		t_join_files[i]=join_files[i];
    		t_join_files_vars[i]=join_files_vars[i];
    	}
		int k=0, k2=0;
		for (int i = 0; i < Q.length; i++) {
			if(lines[i]==0){
				tempQ_files[k]=Q_files[i];
				tempQ_files2[k][0]=Q_files2[i][0];
				tempQ_files2[k][1]=Q_files2[i][1];
				tempQ[k]=Q[i];
				k++;
			}
			else{
				t_join_files[jj+k2]=Q_files2[i][0];
				t_join_files_vars[jj+k2]=name;
				k2++;
			}
				
		}
		join_files=t_join_files;
		join_files_vars=t_join_files_vars;
		Q=tempQ;
		Q_files2=tempQ_files2;
		Q_files=tempQ_files;
		reloadQ();
	}

	private static void updateSize() {
		int j=joins-1; 
		long sum=0;
	    try {
	    	if(fs.isDirectory(new Path("output/Join_"+id+"_"+j))){
		    	FileStatus[] fss = fs.listStatus(new Path("output/Join_"+id+"_"+j));
		        for (FileStatus status : fss) {
		            Path path = status.getPath();
		            if(path.getName().contains("part"))
		            	sum+=fs.getContentSummary(path).getLength();
		        }
	    	}
	    	else{
	    		sum+=fs.getContentSummary(new Path("output/Join_"+id+"_"+j)).getLength();
	    	}

			for (int i = 0; i < Q.length; i++) {
				if(Q_files2[i][1].equals("$$")){
					long siz = new Long(sum/200);
					Q_files2[i][1]= Long.toString(siz);
				} 
					
			}
	    } catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void insertLines(String[] temp, int tempi) {
		int size=Q.length+tempi;
		String[] temp_files= new String[tempi];
		for (int i = 0; i < tempi; i++) {
			temp_files[i]="output/Join_"+id+"_"+joins+":"+(i+1);
		}
		String[] tempQ= new String[size];
		String[] tempQ_files= new String[size];
		String[][] tempQ_files2= new String[size][2];
		for (int i = 0; i < size; i++) {
			if(i<Q.length){
				tempQ[i]=Q[i];
				tempQ_files[i]=Q_files[i];
				tempQ_files2[i][0]=Q_files2[i][0];
				tempQ_files2[i][1]=Q_files2[i][1];
			}
			else{
				tempQ[i]=temp[i-Q.length];
				tempQ_files[i]=temp_files[i-Q.length];
				tempQ_files2[i][0]=temp_files[i-Q.length];
				tempQ_files2[i][1]="$$";
				
			}
		}
		Q=tempQ;
		Q_files2=tempQ_files2;
		Q_files=tempQ_files;
		reloadQ();
	}
	
	private static void reloadQ() {
		newVaRS(vars);
		form2(Q);
	}

	public static String getinpVars(int no) {
		
		return inpVars[no];
	}

	public static void setid(String id1) {
		id=id1+"";
	}

	public static String getTable() {
		
		return tableName;
	} 
	
	public static void setTable(String t) {
		try {
			filters= new HashMap<String, List<ExprFunction>>();
			tableName=t;
			hconf= HBaseConfiguration.create();
			table =new HTable( hconf, t );
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getOutputFile() {
		int j=joins-1;
		return "output/Join_"+id+"_"+j;
	}

	public static void filter(String var, ExprFunction exprFunction) {
		List<ExprFunction> t = filters.get(var);
		if(t==null){
			LinkedList<ExprFunction> t1 = new LinkedList<ExprFunction>();
			t1.add(exprFunction);
			filters.put(var, t1);
		}
		else{
			t.add(exprFunction);
			filters.put(var, t);
		}
	}
}