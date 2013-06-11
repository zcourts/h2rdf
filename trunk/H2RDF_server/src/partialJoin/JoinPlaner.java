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

import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprFunction;

public class JoinPlaner {
	private static Hashtable<String, Integer> hvars, hvars_temp;
	private static String[] Q;
	private static String[] Q_files, join_files, join_files_vars, inpVars, join_files_vars2;
	private static Scan[] ScanBgp;
	private static String[][] Q_files2;
	private static List<Ecount> varsEcount;
	private static Set<Var> vars;
	public static Integer joins;
	private static final int totsize=ByteValues.totalBytes;
	private static float fulljoincost;
	private static String joinpat, tableName;
	private static FileSystem fs;
	private static Query query;
	private static byte[] SUBCLASS ;
	private static byte[] TYPE;
	private static Configuration hconf;
	public static String id="";
	private static HTable table =null, stats_table=null;
	public static HashMap<String,List<ExprFunction>> filters;
	public static Configuration joinConf;
	private static String pool;
	private static String algo;
	
	public static void setQuery(Query q){
		query = q;
	}
	
	public static Scan getScan(int i){
		return ScanBgp[i];
	}
	
	public static void form(Triple[] q2) throws Exception {
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Random g2 = new Random();
		//rani = g2.nextInt(Integer.MAX_VALUE);
		joins=0;
    	Q_files= new String[q2.length];
    	Q_files2= new String[q2.length][2];
    	ScanBgp = new Scan[q2.length];
    	inpVars = new String[q2.length];
		for (int i = 0; i < q2.length; i++) {
        	Q_files[i]="output/BGP"+i;
        	Q_files2[i][0]="output/BGP"+i;
        	ScanBgp[i]=QueryProcessor.getScan(q2[i]);
        	inpVars[i]=QueryProcessor.getInpVars();
        	
        	if(algo.contains("2")){
        		Q_files2[i][1]="5000000";
        	}
        	else{
        		Q_files2[i][1]=getBGPSize(ScanBgp[i]);
        	}
    		System.out.println(Q_files2[i][0]+" "+Q_files2[i][1]);
    		/*Q_files2[0][1]= "8000000";
    		Q_files2[1][1]= "40";
    		Q_files2[2][1]= "4000000";
    		Q_files2[3][1]= "8000000";
    		Q_files2[4][1]= "4000000";
    		Q_files2[5][1]= "4000000";
    		Q_files2[6][1]= "8000000";*/
    		
        	/*Q_files2[0][1]= "100000";
        	Q_files2[1][1]= "50000";
        	
        	if(Q_files2.length==2 && bgp[0].contains("Professor")){
	        	Q_files2[0][1]= "100000";
	        	Q_files2[1][1]= "50000";
        	}
        	else if(Q_files2.length==6 && bgp[1].contains("Professor")){
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
        		Q_files2[0][1]= "1000000";
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
        	
        	
        	//Q_files2[i][1]="10";
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
        for (int i = 0; i < q2.length; i++) {
			formT(q2[i]);
		}
	}

	private static String getBGPSize(Scan scan) throws NotSupportedDatatypeException {
		byte[] rowid = scan.getStartRow();
		Long size= new Long(0);
		System.out.println(Bytes.toStringBinary(rowid));
		TYPE = ByteValues.getFullValue("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
		byte[] prefix = new byte [1+TYPE.length];
		prefix[0]=(byte)3;
		for (int i = 1; i < prefix.length; i++) {
			prefix[i]=TYPE[i-1];
		}
		
		if(Bytes.startsWith(rowid, prefix)){//type queries are not selective
			return "5000000";
		}
		/*if(scan.hasFamilies() && Bytes.toString(scan.getFamilies()[0]).contains("|") ){
			rowid= new byte[1+totsize];
			byte[] temp = scan.getStartRow();
			
			for (int i = 0; i < rowid.length; i++) {
				rowid[i]=temp[i];
			}
		}
		else{
			rowid=scan.getStartRow();
		}*/
		Get get = new Get(rowid);
		try {
			Result result  = stats_table.get(get);
			if(result.size()!=0){
				size = Bytes.toLong(result.raw()[0].getValue());
				//KeyValue[] v = result.raw();
				//size = Bytes.toLong(v[0].getQualifier());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(size==0){
			return "50";
		}
		else{
			return size.toString();
		}
	}

	
	public static String[] form2(String[] bgp) {
		for (int i = 0; i < bgp.length; i++) {
			bgp[i]=form(bgp[i]);
		}
        
		return bgp;
	}
	
	public static void newVaRS(Set<Var> vars2) {
		vars=vars2;
		hvars = new Hashtable<String, Integer>();
		Iterator<Var> it = vars.iterator();
		while(it.hasNext()){
			hvars.put(it.next().toString(false), new Integer(0));
		}
	}
	
	private static String form(String pt) {
		StringTokenizer tokenizer = new StringTokenizer(pt);
		String ret="", s;
		System.out.println(pt);
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
	
	private static void formT(Triple pt) {
		
		if(pt.getSubject().isVariable()){
			String s= pt.getSubject().toString(false);
			Integer no = hvars.get(s);
			no=no+1;
			hvars.put(s, no);
		}
		if(pt.getPredicate().isVariable()){
			String s= pt.getPredicate().toString(false);
			Integer no = hvars.get(s);
			no=no+1;
			hvars.put(s, no);
		}
		if(pt.getObject().isVariable()){
			String s= pt.getObject().toString(false);
			Integer no = hvars.get(s);
			no=no+1;
			hvars.put(s, no);
		}
	}
	
	public static void printVars() {
		System.out.println(hvars);
	}

	public static void removeNonJoiningVaribles(Triple[] bgp) {
		Q= new String[bgp.length];
		for (int i = 0; i < bgp.length; i++) {
			String temp="";
			if(bgp[i].getSubject().isVariable()){
				String st= bgp[i].getSubject().toString(false);
				if( hvars.get(st)>=2){
					temp += st+" ";
				}
			}
			if(bgp[i].getPredicate().isVariable()){
				String st= bgp[i].getPredicate().toString(false);
				if( hvars.get(st)>=2){
					temp += st+" ";
				}
			}
			if(bgp[i].getObject().isVariable()){
				String st= bgp[i].getObject().toString(false);
				if( hvars.get(st)>=2){
					temp += st+" ";
				}
			}
			Q[i]=temp;
		}
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
				if (size<=20000) //selective
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

	public static String getNextJoin() throws NotSupportedDatatypeException {
		joinConf = new Configuration();
		//joinConf.set("mapred.fairscheduler.pool", pool);
		joinpat="";
    	join_files = new String[0];
    	join_files_vars = new String[0];
    	join_files_vars2 = new String[0];
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
		//Path vars=new Path("input/JoinVars_"+id+"_"+joins);
		String[] lines=null;
		String[] sfiles=null;
		boolean mapReduceTranslate=false;
		try {
			if (fs.exists(outFile)) {
				fs.delete(outFile,true);
			}

			/*if (fs.exists(vars)) {
				fs.delete(vars,true);
			}
			FSDataOutputStream v= fs.create(vars);*/
			lines=printJoinV(joinConf, ret);
			for (int i = 0; i < lines.length; i++) {
				System.out.println(lines[i]);
			}
			sfiles = splitFiles(join_files,lines);
			joinConf.set("input.retno",retno);
			joinConf.set("input.joins",joins+"");
			//v.writeBytes(retno+"\n");
			//v.writeBytes(joins+"\n");
			insertLines(temp, tempi);
			joins++;
			printQ();
			if(isEmpty()){
				if(joinSize>=70000000){//MapReduce translate
					//v.writeBytes("0\n");
					//v.writeBytes("\n");
					mapReduceTranslate=true;
					joinConf.set("input.last","2");
					joinConf.set("input.resultVars",query.getResultVars().toString().replace(',', ' ').replace('[', ' ').replace(']', ' ').trim());
					//v.writeBytes("2\n");
					//v.writeBytes(query.getResultVars().toString().replace(',', ' ').replace('[', ' ').replace(']', ' ').trim()+"\n");
				}
				else{//index translate
					joinConf.set("input.last","1");
					joinConf.set("input.resultVars",query.getResultVars().toString().replace(',', ' ').replace('[', ' ').replace(']', ' ').trim());
					//v.writeBytes("1\n");
					//v.writeBytes(query.getResultVars().toString().replace(',', ' ').replace('[', ' ').replace(']', ' ').trim()+"\n");
				}
			}
			else{
				joinConf.set("input.last","0");
				joinConf.set("input.resultVars","");
				//v.writeBytes("0\n");
				//v.writeBytes("\n");
			}
			printNonJoinV(joinConf, ret, lines);
			
			//v.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println(joinSize);
		if(algo.contains("3")){
			SerialQueryProcessorThreaded.executeJoin(outFile, sfiles, joinConf);
		}
		else if(algo.contains("4")){
			if(joinSize >= 40000)
				QueryProcessor.executeJoin(outFile, sfiles, joinConf);
			else
				SerialQueryProcessorThreaded.executeJoin(outFile, sfiles, joinConf);
		}
		else if(algo.contains("1")||algo.contains("2")){//only mapreduce
			QueryProcessor.executeJoin(outFile, sfiles, joinConf);
		}
		updateSize();
		if(mapReduceTranslate){
			System.out.println("MrTranslate");
			Path transout1 = new Path("output/Translate_"+id+"_1");
			Path transout2 = new Path("output/Translate_"+id+"_2");
			Translator.executeTranslate1(new Path("output/Join_"+id+"_"+(joins-1)), transout1);
			Translator.executeTranslate2(transout1, transout2);
		}
		
		/*try {
			if (fs.exists(vars)) {
				fs.delete(vars,true);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		
		deleteJoinFiles();
		return ret;
	}
	


	private static float joinCost(String var, Boolean isTotalEllimination) {
		float sel_sum=0, tot_sum=0, non_sel=0;
		int index_access=1;
		String joinInSel="", joinInTot="";
		for (int i = 0; i < Q.length; i++) {
			if(Q[i].contains(var)){
				long size = Long.parseLong(Q_files2[i][1]);
				if (size<=20000){ //selective
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

	private static void printNonJoinV(Configuration joinConf, String ret, String[] lines) {
		//try {
			int s=0;
			for (int i = 0; i < join_files.length; i++) {
				if(lines[i].contains("|")){
					if(lines[i].contains("J"))
						System.exit(1);
					String fname=lines[i].substring(0, lines[i].indexOf("|"));
					joinConf.set("input.reduceScans."+s+".fname", fname);
					//Bytes.writeByteArray(v, Bytes.toBytes(fname));
					int id=Integer.parseInt(lines[i].substring(lines[i].indexOf("P")+1));//String.valueOf(lines[i].charAt(lines[i].length()-1)));
					Scan scan = getScan(id);
					joinConf.set("input.reduceScans."+s+".startrow", Bytes.toStringBinary(scan.getStartRow()));
					//Bytes.writeByteArray(v, scan.getStartRow());
					if (scan.hasFamilies()) {
						System.out.println(Bytes.toString(scan.getFamilies()[0]));
						joinConf.set("input.reduceScans."+s+".columns", Bytes.toString(scan.getFamilies()[0]));
						//Bytes.writeByteArray(v, scan.getFamilies()[0]);//Bytes.toBytes(getScan(id).getInputColumns()));
					}else{
						System.out.println("no");
						joinConf.set("input.reduceScans."+s+".columns", "");
						//Bytes.writeByteArray(v, Bytes.toBytes(""));//Bytes.toBytes(getScan(id).getInputColumns()));
					}
					s++;
				}
			}
			joinConf.setStrings("input.reduceScans", s+"");
			//Bytes.writeByteArray(joinConf, Bytes.toBytes("end"));
		//} catch (IOException e) {
		//	e.printStackTrace();
		//}
	}

	private static String[] printJoinV(Configuration joinConf, String ret) {
		HashMap<String, Integer> varSet = new HashMap<String, Integer>();
		String[] lines = new String[join_files.length];
		for (int i = 0; i < join_files.length; i++) {
			lines[i]="{";
		}
		//String joinpat = query.getResultVars().get(query.getResultVars().size()-1).toString();
		//try {
			joinConf.set("input.joinvars", ret);
			//v.writeBytes(ret+"\n");
			String patId="";
			for (int i = 0; i < join_files.length; i++) {
				String jf = join_files[i];
				//if(!varSet.contains(join_files_vars[i])){
				StringTokenizer t = new StringTokenizer(join_files_vars2[i]);
				while(t.hasMoreTokens()){
					String s=t.nextToken();
					if(!ret.contains(s)){
						if (!varSet.containsKey(s)) {
							varSet.put(s, 1);
						}
						else{
							Integer temp = varSet.get(s);
							temp++;
							varSet.put(s, temp);
						}
					}
				}
				
				if(jf.contains("BGP") && joinpat.contains(jf.split("BGP")[1])){
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
					patId+=id+" "+join_files_vars[i]+" $$ ";
					//v.writeBytes(id+" "+join_files_vars[i]+" $$ ");
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
						patId+=id+" "+join_files_vars[i]+" $$ ";
						//v.writeBytes(id+" "+join_files_vars[i]+" $$ ");
					}
				}
			}
			joinConf.set("input.patId", patId);

			Iterator<String> it = varSet.keySet().iterator();
			int c =0;
			while(it.hasNext()){
				String s =it.next();
				int snum = varSet.get(s);
				if(snum>=2){
					c++;
					joinConf.set("input.double"+c, s);
					joinConf.setInt("input.double"+c+".num", snum);
				}
			}
			joinConf.setInt("input.double", c);
			
			//v.writeBytes("\n");
		/*} catch (IOException e) {
			e.printStackTrace();
		}*/
		return lines;
	}

	private static void deleteJoinFiles() {
		for (int i = 0; i < join_files.length; i++) {
			try {
				if(join_files[i].contains("Join"))
					fs.delete(new Path(join_files[i].substring(0, join_files[i].lastIndexOf(":"))),true);
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
		String[] t_join_files_vars2 = new String[Q.length-j+jj];
    	for (int i = 0; i < jj; i++) {
    		t_join_files[i]=join_files[i];
    		t_join_files_vars[i]=join_files_vars[i];
    		t_join_files_vars2[i]=join_files_vars2[i];
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
				t_join_files_vars2[jj+k2]=Q[i];
				k2++;
			}
				
		}
		join_files=t_join_files;
		join_files_vars=t_join_files_vars;
		join_files_vars2=t_join_files_vars2;
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
			table.flushCommits();
			if(t.contains("10k")){
				stats_table =new HTable( hconf, t+"stats" );
			}
			else{
				stats_table =new HTable( hconf, t+"_stats" );
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String getOutputFile() {
		int j=joins-1;
		if(j>0)
			return "output/Join_"+id+"_"+j;
		else
			return "output/Join_"+id+"_0";
			
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

	public static void setTable(String t, String algo1, String pool1) {
		try {
			pool=pool1;
			algo=algo1;
			filters= new HashMap<String, List<ExprFunction>>();
			tableName=t;
			hconf= HBaseConfiguration.create();
			table =new HTable( hconf, t );
			table.flushCommits();
			if(t.contains("10k")){
				stats_table =new HTable( hconf, t+"stats" );
			}
			else{
				stats_table =new HTable( hconf, t+"_stats" );
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
