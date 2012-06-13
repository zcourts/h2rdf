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

import input_format.TableColumnSplit;
import input_format.TableMapReduceUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.JenkinsHash;

import concurrent.ThreadedH2RDFClient;

import byte_import.MyNewTotalOrderPartitioner;
import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

public class SerialQueryProcessorThreaded {
	public static Hashtable<String,ValueDoubleMerger>[] bindings;
	public static Hashtable<String, Integer> jNo, joinVarNames;
	public static Text outKey = new Text();
	public static Text outValue = new Text();
	public static String jo;
	public static String newjoinVars;
	public static Configuration hconf;
	public static String joinNo;
	public static String resultVars;
	public static String[] nonJoinVarNames;
	public static String[][] nonJoinCol;
	public static byte[][][] nonJoinStartRow;
	public static int[] nonJoinSizeTab;
	public static int j, nonJoinSize;
	public static int isLast,type;
	public static HTable table;
	public static FSDataOutputStream out= null;
	public static byte[] SUBCLASS ;//Bytes.toBytes( new Long("8742859611446415633"));
	public static FileSystem fs=null;
	public static Configuration conf;
	public static final int totsize= ByteValues.totalBytes, rowlength=1+2*totsize, MAX_THREADS=40;
	
	public static int executeJoin(Path outFile, Object[] join_files, Configuration joinConf) throws NotSupportedDatatypeException {
		
		int ret=0;
		conf=joinConf;
	    initialize(outFile, joinConf);
		System.out.println("Serial Join........");
	    for (int i = 0; i < join_files.length; i++) {
	    	System.out.println(((String)join_files[i]));
	    	int no=Integer.parseInt(((String)join_files[i]).substring(((String)join_files[i]).length()-1));
	    	if(((String)join_files[i]).contains("BGP")){

	    		joinScan(JoinPlaner.getScan(no),JoinPlaner.getinpVars(no),"P"+no);
	    	}
	    	else{ 
	    		joinFile(((String)join_files[i]).split(":")[0]);
	    	}
	    }
		System.out.println("print out..");
		printJoin(j);
	    
		ret= out.size();
		ret = 0;
		try {
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static void initialize(Path outFile, Configuration joinConf) {
		bindings = null; 
		Configuration conf = new Configuration();
    	try {
			SUBCLASS = ByteValues.getFullValue("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
    		hconf=HBaseConfiguration.create();
			table = new HTable( hconf, JoinPlaner.getTable() );
        	fs = FileSystem.get(joinConf);
        	jo=joinConf.get("input.joinvars");
	    	newjoinVars=joinConf.get("input.patId");
	    	joinNo=joinConf.get("input.retno");
	    	j=Integer.parseInt(joinConf.get("input.joins"));
	    	isLast=Integer.parseInt(joinConf.get("input.last"));
	    	resultVars=joinConf.get("input.resultVars");
	    	
	    	String temp=null;
	    	nonJoinVarNames = new String[5];
	    	nonJoinCol = new String[5][10];
	    	nonJoinStartRow = new byte[5][10][];
	    	nonJoinSize=0;
	    	nonJoinSizeTab=new int[5];
	    	for (int i = 0; i < nonJoinSizeTab.length; i++) {
	        	nonJoinSizeTab[i]=0;
			}

    		System.out.println(joinConf.get("input.reduceScans") );
	    	for (int i = 0; i < Integer.parseInt(joinConf.get("input.reduceScans")); i++) {
	    		temp=joinConf.get("input.reduceScans."+i+".fname");
	    		System.out.println(temp );
	    		int id=getvarind(temp);
	    		if(id==-1){
	    			nonJoinVarNames[nonJoinSize]=temp;
	    			id=nonJoinSize;
	    			nonJoinSize++;
	    		}
	    		System.out.println(id );
	    		byte[] rowid= Bytes.toBytesBinary(joinConf.get("input.reduceScans."+i+".startrow"));
	    		System.out.println(Bytes.toStringBinary(rowid));

				int ffound=0;
				byte[] subclasses = new byte[100];
				
				if (rowid.length==rowlength) {
					byte[] objid = new byte[totsize];
					for (int i1 = 0; i1 < totsize; i1++) {
						objid[i1]=rowid[i1+totsize+1];
					}
					byte[] classrowStart = new byte[rowlength+2];
					byte[] classrowStop = new byte[rowlength+2];
					classrowStart[0]=(byte)3; //pos
					for (int i1 = 0; i1 < totsize; i1++) {
						classrowStart[i1+1]=SUBCLASS[i1];
					}
					for (int i1 = 0; i1 < totsize; i1++) {
						classrowStart[i1+totsize+1]=objid[i1];
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
					for (int i1 = 0; i1 < a.length; i1++) {
						bid[i]=a[i];
					}
					Scan scan1 =new Scan();
					scan1.setStartRow(classrowStart);
					scan1.setStopRow(classrowStop);
					scan1.setCaching(70000);
					scan1.setCacheBlocks(true);
					scan1.addFamily(bid);
					ResultScanner resultScanner=null;
					try {
						resultScanner = table.getScanner(scan1);
						Result result = null;
						while((result=resultScanner.next())!=null){
							System.out.println("Subclasses: "+result.size());
							Iterator<KeyValue> it = result.list().iterator();
							while(it.hasNext()){
								KeyValue kv = it.next();
								byte[] qq = kv.getQualifier();
								for (int i1 = 0; i1 < totsize; i1++) {
									subclasses[ffound*totsize + i1]=qq[i1];
								}
								ffound++;
							}
							
						}
					} catch (IOException e) {
						e.printStackTrace();
					}finally {
						resultScanner.close();  // always close the ResultScanner!
					}
				}
				
				if(ffound>0){
					nonJoinStartRow[id][nonJoinSizeTab[id]] = new byte[ffound*totsize+rowid.length];
					for (int i1 = 0; i1 < ffound*totsize+rowid.length; i1++) {
						if(i1>=rowlength && i1< ffound*totsize+rowlength)
							nonJoinStartRow[id][nonJoinSizeTab[id]][i1]=subclasses[i1-rowlength];
						else if(i1<rowlength)
							nonJoinStartRow[id][nonJoinSizeTab[id]][i1]=rowid[i1];
					}	
				}
				else{
					nonJoinStartRow[id][nonJoinSizeTab[id]] = new byte[rowid.length];
					for (int i1 = 0; i1 < rowid.length; i1++) {
						nonJoinStartRow[id][nonJoinSizeTab[id]][i1]=rowid[i1];
					}
				}
	    		nonJoinCol[id][nonJoinSizeTab[id]]=joinConf.get("input.reduceScans."+i+".columns");
	    		nonJoinSizeTab[id]++;
	    	
	    		/*nonJoinStartRow[id][nonJoinSizeTab[id]] = new byte[rowid.length];
				for (int i2 = 0; i2 < rowid.length; i2++) {
					nonJoinStartRow[id][nonJoinSizeTab[id]][i2]=rowid[i2];
				}
	    		nonJoinCol[id][nonJoinSizeTab[id]]=joinConf.get("input.reduceScans."+i+".columns");
	    		nonJoinSizeTab[id]++; //without subclasses
	    		*/
			}
	    	
	    	StringTokenizer list=new StringTokenizer(jo);
	    	StringTokenizer list2=new StringTokenizer(joinNo);
    		joinVarNames = new Hashtable<String, Integer>();
    		jNo = new Hashtable<String, Integer>();
    		int jVars=0; 
    		while (list.hasMoreTokens()) {
    			String tempv =list.nextToken();
    			joinVarNames.put(tempv, new Integer(jVars));
    			jNo.put(tempv, Integer.parseInt(list2.nextToken()));
    			jVars++;
    		}
    		bindings = new Hashtable[jVars];
    		for (int i = 0; i < jVars; i++) {
    			bindings[i]= new Hashtable<String, ValueDoubleMerger>();
			}
	    	out = fs.create(outFile);
    	} catch (IOException e) {
			e.printStackTrace();
		} catch (NotSupportedDatatypeException e) {
			e.printStackTrace();
		}	
		
	}
	
	public static void joinFile(String inputFile) {
		String line;
		try {
			if(fs.isDirectory(new Path(inputFile))){
				FileStatus[] fss;
					fss = fs.listStatus(new Path(inputFile));
		        for (FileStatus status : fss) {
		            Path path = status.getPath();
		            if(path.getName().contains("part")){
		            	
						//Path path = new Path(p[i]+"/part-00000");
						FSDataInputStream in=null;
						if(fs.exists(path)){
							in = fs.open(path);
						}
						
						while((line = in.readLine())!=null){
							StringTokenizer tokenizer = new StringTokenizer(line);
							String pat = tokenizer.nextToken("!");
							String joinVars = newjoinVars.split(pat)[1];
							joinVars=joinVars.substring(0, joinVars.indexOf("$$")-1);
							StringTokenizer tok;
							String newline="";
							String else_value="";
							int else_size=0;
							while (tokenizer.hasMoreTokens()) {
								String binding=tokenizer.nextToken("!");
								if(binding.startsWith("?")){
									tok=new StringTokenizer(binding);
									String pred=tok.nextToken("#");
									if(joinVars.contains(pred)){
										pred+="#";
										if(!tok.hasMoreTokens()){
											return;
										}
										String b = tok.nextToken("#");
										newline=breakList(newline, b, pred);
											
									}
									else{
										else_value+=binding+"!";
										newline+=binding+"!";
										else_size++;
									}
								}
							}
							if(else_size==0)
								else_value="";
							
							
							tokenizer = new StringTokenizer(newline);
							while (tokenizer.hasMoreTokens()) {
								String binding = tokenizer.nextToken("!");
								tok=new StringTokenizer(binding);
								String jvar=tok.nextToken("#");
								if(joinVars.contains(jvar)){
									//join variable
									//System.out.println(binding);
									outKey.set(binding);
									outValue.set(else_value);
									collect(binding, pat, else_value);
								}
							}
						}
		            }
		        }
			}
			else{
				FSDataInputStream in=fs.open(new Path(inputFile));
				while((line = in.readLine())!=null){
					StringTokenizer tokenizer = new StringTokenizer(line);
					String pat = tokenizer.nextToken("!");
					String joinVars = newjoinVars.split(pat)[1];
					joinVars=joinVars.substring(0, joinVars.indexOf("$$")-1);
					StringTokenizer tok;
					String newline="";
					String else_value="";
					int else_size=0;
					while (tokenizer.hasMoreTokens()) {
						String binding=tokenizer.nextToken("!");
						if(binding.startsWith("?")){
							tok=new StringTokenizer(binding);
							String pred=tok.nextToken("#");
							if(joinVars.contains(pred)){
								pred+="#";
								if(!tok.hasMoreTokens()){
									return;
								}
								//byte[] b = Bytes.toBytes(tok.nextToken("#").toCharArray());
								String b = tok.nextToken("#");
								newline=breakList(newline, b, pred);
									
							}
							else{
								else_value+=binding+"!";
								newline+=binding+"!";
								else_size++;
							}
						}
					}
					if(else_size==0)
						else_value="";
					
					tokenizer = new StringTokenizer(newline);
					while (tokenizer.hasMoreTokens()) {
						String binding = tokenizer.nextToken("!");
						tok=new StringTokenizer(binding);
						String jvar=tok.nextToken("#");
						if(joinVars.contains(jvar)){
							//join variable
							//System.out.println(binding+" "+else_value);
							outKey.set(binding);
							outValue.set(else_value);
							collect(binding, pat, else_value);
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public static void joinScan(Scan inscan, String vars, String pat) throws NotSupportedDatatypeException {
		StringTokenizer vtok = new StringTokenizer(vars);
		type=0;
		while(vtok.hasMoreTokens()){
			vtok.nextToken();
			type++;
		}
		String joinVars = newjoinVars.split(pat)[1];
		joinVars=joinVars.substring(0, joinVars.indexOf("$$")-1);
		//String col=Bytes.toString(inscan.getFamilies()[0]);
		byte[] rowid =inscan.getStartRow();
		byte[] startr = new byte[rowlength+2];
		byte[] stopr = new byte[rowlength+2];
		if(type==1){
			startr[0] =rowid[0];
			stopr[0] =rowid[0];
			for (int i = 1; i < rowid.length; i++) {
				startr[i] =rowid[i];
				stopr[i] =rowid[i];
			}
			if (rowid.length==rowlength) {
				startr[startr.length-2] =(byte)0;
				startr[startr.length-1] =(byte)0;
				stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			}
		}
		else if(type==2){
			if (rowid.length==totsize+1) {
				startr[0] =rowid[0];
				stopr[0] =rowid[0];
				for (int i = 1; i < rowid.length; i++) {
					startr[i] =rowid[i];
					stopr[i] =rowid[i];
				}
				  for (int i = totsize+1; i < startr.length-2; i++) {
					  startr[i] =(byte)0;
					  stopr[i] =(byte)255;
				  }
				  startr[startr.length-2] =(byte)0;
				  startr[startr.length-1] =(byte)0;
				  stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				  stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			}
			else{
				byte[] stop = inscan.getStopRow();
				startr[0] =rowid[0];
				stopr[0] =stop[0];
				for (int i = 1; i < 1+2*totsize; i++) {
					startr[i] =rowid[i];
					stopr[i] =stop[i];
				}
				startr[startr.length-2] =(byte)0;
				startr[startr.length-1] =(byte)0;
				stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
				stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			}
		}
		byte[] bid,a;
		a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}
		System.out.println("startr"+Bytes.toStringBinary(startr));
		System.out.println("stopr"+Bytes.toStringBinary(stopr));
		Scan scan =new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		
		//must be changed
		//scan.addFamily(bid);
		ResultScanner resultScanner=null;
		try {
			resultScanner = table.getScanner(scan);
			Result result = null;
			while((result=resultScanner.next())!=null){
				//System.out.println("size: "+result.size());
				Iterator<KeyValue> it = result.list().iterator();
				while(it.hasNext()){
					KeyValue kv = it.next();
					if(kv.getQualifier().length==totsize){
						if(type==1){
							  StringTokenizer vtok2 = new StringTokenizer(vars);
							  String v1=vtok2.nextToken();
							  collect(v1+"#"+ByteValues.getStringValue(kv.getQualifier())+"_", 
									  pat, "");
						}
						else if(type==2){
							  StringTokenizer vtok2 = new StringTokenizer(vars);
							  String v1=vtok2.nextToken();
							  String v2=vtok2.nextToken();
							  byte[] r = kv.getRow();
							  byte[] r1= new byte[totsize];
							  for (int j = 0; j < r1.length; j++) {
								  r1[j]=r[totsize+1+j];
							  }
							  if(joinVars.contains(v1)){
								  collect(v1+"#"+ByteValues.getStringValue(r1)+"_", 
										  pat, 
										  v2+"#"+ByteValues.getStringValue(kv.getQualifier())+"_!");
								  //System.out.println(v1+"#"+ByteValues.getStringValue(r1)+ 
										  
									//	  v2+"#"+ByteValues.getStringValue(kv.getQualifier())+"_");
							  }
							  else if(joinVars.contains(v2)){
								  collect(v2+"#"+ByteValues.getStringValue(kv.getQualifier())+"_", 
										  pat, 
										  v1+"#"+ByteValues.getStringValue(r1)+"_!");
								  //System.out.println(v2+"#"+ByteValues.getStringValue(kv.getQualifier())+ 
										  
										  //v1+"#"+ByteValues.getStringValue(r1)+"_");
							  }
						}
					}
					
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}

		
	}


	public static void printJoin( int joinid) throws NotSupportedDatatypeException {
		String var ;
		Integer i=0, in_no;
		Enumeration<String> varnames = joinVarNames.keys();
		//System.out.println("print");
		ThreadedProcessor[] thread= new ThreadedProcessor[MAX_THREADS];
		int max_th=MAX_THREADS;
		while(varnames.hasMoreElements()){
			String sum="J"+joinid+":"+(i+1)+"!";
			var = varnames.nextElement().toString();
			in_no=(Integer)jNo.get(var);
			String[] keyTable = new String[0];
			keyTable=bindings[i].keySet().toArray(keyTable);
			int size =keyTable.length/MAX_THREADS+1;
			if(size<20){
				max_th=keyTable.length/20;
				if(max_th==0)
					max_th=1;
				size =keyTable.length/max_th+1;
			}
			for (int threadId = 0; threadId < max_th; threadId++) {
				thread[threadId] = new ThreadedProcessor(threadId, size, keyTable, bindings[i], in_no, var, sum);
				thread[threadId].start();
			}
			i++;
		}
		System.out.print("Wait for "+max_th+" worker threads to complete\n");
	      for (i=0; i <max_th; i++) {
	    	  try {
	            thread[i].join();
	         }
	         catch (InterruptedException e) {
	            System.out.print("Join interrupted\n");
	         }
	      }
	 
	      System.out.print("Join completed\n");
	}

	public static void collect(String binding, String pat, String else_val) {
		//System.out.println("pat="+pat+" binding="+binding+" else_val="+else_val);
		String tpat, el;
		ValueDoubleMerger value;

		StringTokenizer t = new StringTokenizer(binding);
		String keyVar = t.nextToken("#");
		String b = t.nextToken("#");
		StringTokenizer t2 = new StringTokenizer(b);
		while(t2.hasMoreTokens()){
			String b2= keyVar+"#"+t2.nextToken("_")+"_";
			Integer index = joinVarNames.get(keyVar);
			if(bindings[index].containsKey(b2)){
				value = bindings[index].get(b2);
				value.merge(else_val, pat);
				bindings[index].put(b2, value);
				/*StringTokenizer t1 = new StringTokenizer(value);
				tpat=t1.nextToken("$");
				if(t1.hasMoreTokens()){
					el=t1.nextToken("$");
				}
				else{
					el="";
				}
				if(!tpat.contains(pat)){
					tpat+=pat+"_";
				}
				el+=else_val;
				bindings[index].put(b2, tpat+"$"+el);*/
			}
			else{
				ValueDoubleMerger merger = new ValueDoubleMerger(conf);
				merger.merge(else_val, pat);
				bindings[index].put(b2, merger);
				//bindings[index].put(b2, pat+"_$"+else_val);
			}
		}
	}

	public static String breakList(String newline, String binding, String pred) {
		
		StringTokenizer tokenizer = new StringTokenizer(binding);
		while(tokenizer.hasMoreTokens()) {
			String temp1 = tokenizer.nextToken("_");
			newline+=pred+temp1+"_!";
		}
		return newline;
	}

	
	public static int reduceJoinAllVar(byte pinakas, byte[] b1, byte[] b2, byte[] b3) {
		int ret=0;
		byte[] startr= new byte[rowlength+2];
		byte[] stopr= new byte[rowlength+2];
		startr[0]=pinakas;
		stopr[0]=pinakas;
		for (int i1 = 0; i1 < totsize; i1++) {
			startr[i1+1]=b1[i1];
			stopr[i1+1]=b1[i1];
		}
		for (int i1 = 0; i1 < totsize; i1++) {
			startr[i1+totsize+1]=b2[i1];
			stopr[i1+totsize+1]=b2[i1];
		}
		startr[startr.length-2] =(byte)0;
		startr[startr.length-1] =(byte)0;
		stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		Scan scan = new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		scan.addColumn(Bytes.toBytes("A"), b3);
		ResultScanner resultScanner=null;
		try {
			resultScanner = table.getScanner(scan);
			Result re;
			while((re = resultScanner.next())!=null){
				if(re.size()!=0){
					ret++;
				}
				if(ret>0){
					ret=1;
					break;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}
		return ret;
	}
	
	
	public static String reduceJoin(byte pinakas, byte[] b1, byte[] b2,
			String varname) throws NotSupportedDatatypeException {
		String ret="";
		byte[] startr= new byte[rowlength+2];
		byte[] stopr= new byte[rowlength+2];
		startr[0]=pinakas;
		stopr[0]=pinakas;
		for (int i1 = 0; i1 < totsize; i1++) {
			startr[i1+1]=b1[i1];
			stopr[i1+1]=b1[i1];
		}
		for (int i1 = 0; i1 < totsize; i1++) {
			startr[i1+totsize+1]=b2[i1];
			stopr[i1+totsize+1]=b2[i1];
		}
		startr[startr.length-2] =(byte)0;
		startr[startr.length-1] =(byte)0;
		stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		Scan scan = new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		byte[] a, col, bid=null;
		a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}
		scan.addFamily(bid);
		scan.setCacheBlocks(true);
		ResultScanner resultScanner=null;
		//System.out.println("start: "+Bytes.toStringBinary(startr));
		//System.out.println("start: "+Bytes.toStringBinary(stopr));
		try {
			resultScanner = table.getScanner(scan);
			Result re;
			while((re = resultScanner.next())!=null){
				if(re.size()!=0){
					//System.out.println("found");
					KeyValue[] v = re.raw();
					for (int j = 0; j < v.length; j++) {
						ret+=varname+"#"+vtoString(v[j].getQualifier())+"_!";
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			resultScanner.close();  // always close the ResultScanner!
		}
			
		return ret;
	}



	public static void writeOut(Text outKey2) throws NumberFormatException, NotSupportedDatatypeException {
		
		/*if(isLast==1){
    		StringTokenizer list;
    		list=new StringTokenizer(outKey2.toString());
    		
    		String newline = "";
    		list.nextToken("!");
			StringTokenizer tok;
			while (list.hasMoreTokens()) {
				String binding=list.nextToken("!");
				//if(binding.startsWith("?")){
					tok=new StringTokenizer(binding);
					String pred=tok.nextToken("#");
					if(resultVars.contains(pred.substring(1))){
						pred+="$#$";
						if(!tok.hasMoreTokens()){
							System.out.println("wrong format: "+ outKey2);
							System.exit(2);
						}
						//byte[] b = Bytes.toBytes(tok.nextToken("#").toCharArray());
						String b = tok.nextToken("#");
						newline=transform(newline, b, pred);
					}
				//}
			}
			outKey2.set(newline);
			System.out.println(outKey2);
			
    	}*/
		
		try {
			synchronized(out){
				out.writeBytes(outKey2+"\n");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	

	public static boolean findDouble(int vid) {
		boolean ret =false;
		int nonjno=nonJoinSizeTab[vid];
		for (int jj = 0; jj < nonjno; jj++) {
			if(nonJoinCol[vid][jj].contains("?")){
				return true;
			}
		}
		return ret;
	}

	public static String transform(String newline, String binding, String pred) throws NumberFormatException, NotSupportedDatatypeException {
		
		String bindings="";
		boolean found=false;
		//System.out.println(binding);
		StringTokenizer tokenizer = new StringTokenizer(binding);
		byte[] k = null;
		while(tokenizer.hasMoreTokens()) {
			String temp = tokenizer.nextToken("_");
			//System.out.println(temp);
			//System.out.println(binding);
			bindings+=ByteValues.translate((byte) new Byte(temp.substring(0,temp.indexOf("|"))), Bytes.toBytes(Long.parseLong(temp.substring(temp.indexOf("|")+1))), table);
			/*byte[] temp3=Bytes.toBytes(Long.parseLong(temp.substring(temp.indexOf("|")+1)));
			byte[] temp1=new byte[totsize];
			temp1[0]=(byte) new Byte(temp.substring(0,temp.indexOf("|")));
			for (int i = 0; i < totsize-1; i++) {
				temp1[i+1]=temp3[i];
			}
			//byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			k = new byte[totsize+1];
			k[0]=(byte) 1;
			
			for (int j = 0; j < totsize; j++) {
				k[j+1]=temp1[j];
			}
			Get get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					bindings+=Bytes.toString(result.raw()[0].getValue())+"$_$";
					found=true;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
		bindings+="$^^$";
		newline+=pred+bindings;
		/*if(found){
			newline+=pred+bindings;
		}
		else{
			System.out.println("Id not found in names index"+Bytes.toStringBinary(k));
			System.exit(1);
		}*/
		return newline;
	}
	
	public static String vtoString(byte[] value) throws NotSupportedDatatypeException {
		  /*long v = Bytes.toLong(value);
		
		  if(value.length!=totsize){
			  System.out.println(v);
			  System.exit(1);
		  }*/
		  return ByteValues.getStringValue(value)+"_";
		}

	public static int getvarind(String var) {
		for (int i = 0; i < nonJoinVarNames.length; i++) {
			if(var.equals(nonJoinVarNames[i])){
				return i;
			}
		}
		return -1;
	}
}
