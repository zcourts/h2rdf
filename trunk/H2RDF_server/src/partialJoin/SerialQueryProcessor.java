package partialJoin;

import input_format.TableColumnSplit;
import input_format.TableMapReduceUtil;

import java.io.IOException;
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

import byte_import.MyNewTotalOrderPartitioner;

public class SerialQueryProcessor {
	private static Hashtable[] bindings;
	private static Hashtable jNo, joinVarNames;
	private static Text outKey = new Text();
	private static Text outValue = new Text();
	private static String jo;
	private static String newjoinVars;
	private static HBaseConfiguration hconf=new HBaseConfiguration();
	private static String joinNo;
	private static String resultVars;
	private static String[] nonJoinVarNames;
	private static String[][] nonJoinCol;
	private static byte[][][] nonJoinStartRow;
	private static int[] nonJoinSizeTab;
	private static int j, nonJoinSize;
	private static int isLast;
	private static HTable table;
	private static FSDataOutputStream out= null;
	private static byte[] SUBCLASS = Bytes.toBytes( new Long("8742859611446415633"));
	private static FileSystem fs=null;
	
	public static int executeJoin(Path outFile, Object[] join_files) {
		
		int ret=0;
		int m_rc = 0;
	    int ScanSize=0;
	    int JoinSize=0;
	    Scan[] scanList = new Scan[join_files.length];
	    String[] joinFiles = new String[join_files.length];
	    initialize(outFile);
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
		printJoin(out, j);
	    
		ret= out.size();
		try {
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	private static void initialize(Path outFile) {
		bindings = null; 
		Configuration conf = new Configuration();
    	try {
			table = new HTable( hconf, JoinPlaner.getTable() );
        	fs = FileSystem.get(conf);
	    	FSDataInputStream v = fs.open(new Path("input/JoinVars_"+JoinPlaner.id+"_"+(JoinPlaner.joins-1)));
	    	jo=v.readLine();
	    	newjoinVars=v.readLine();
	    	joinNo=v.readLine();
	    	j=Integer.parseInt(v.readLine());
	    	isLast=Integer.parseInt(v.readLine());
	    	resultVars=v.readLine();
	    	String temp=null;
	    	nonJoinVarNames = new String[5];
	    	nonJoinCol = new String[5][10];
	    	nonJoinStartRow = new byte[5][10][];
	    	nonJoinSize=0;
	    	nonJoinSizeTab=new int[5];
	    	for (int i = 0; i < nonJoinSizeTab.length; i++) {
	        	nonJoinSizeTab[i]=0;
			}
	    	while(!(temp=Bytes.toString(Bytes.readByteArray(v))).equals("end")){
	    		int id=getvarind(temp);
	    		if(id==-1){
	    			nonJoinVarNames[nonJoinSize]=temp;
	    			id=nonJoinSize;
	    			nonJoinSize++;
	    		}
	    		byte[] rowid= Bytes.readByteArray(v);
				int ffound=0;
				byte[] subclasses = new byte[100];
				
				if (rowid.length==17) {
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
								for (int i = 0; i < 8; i++) {
									subclasses[ffound*8 + i]=qq[i];
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
					nonJoinStartRow[id][nonJoinSizeTab[id]] = new byte[ffound*8+rowid.length];
					for (int i = 0; i < ffound*8+rowid.length; i++) {
						if(i>=17 && i< ffound*8+17)
							nonJoinStartRow[id][nonJoinSizeTab[id]][i]=subclasses[i-17];
						else if(i<17)
							nonJoinStartRow[id][nonJoinSizeTab[id]][i]=rowid[i];
					}	
				}
				else{
					nonJoinStartRow[id][nonJoinSizeTab[id]] = new byte[rowid.length];
					for (int i = 0; i < rowid.length; i++) {
						nonJoinStartRow[id][nonJoinSizeTab[id]][i]=rowid[i];
					}
				}
	    		//nonJoinStartRow[id][nonJoinSizeTab[id]]=Bytes.readByteArray(v); sos xwris subclass
	    		nonJoinCol[id][nonJoinSizeTab[id]]=Bytes.toString(Bytes.readByteArray(v));
	    		nonJoinSizeTab[id]++;
	    	}
	    	v.close();
	    	
	    	StringTokenizer list=new StringTokenizer(jo);
	    	StringTokenizer list2=new StringTokenizer(joinNo);
    		joinVarNames = new Hashtable();
    		jNo = new Hashtable();
    		int jVars=0; 
    		while (list.hasMoreTokens()) {
    			String tempv =list.nextToken();
    			joinVarNames.put(tempv, new Integer(jVars));
    			jNo.put(tempv, Integer.parseInt(list2.nextToken()));
    			jVars++;
    		}
    		bindings = new Hashtable[jVars];
    		for (int i = 0; i < jVars; i++) {
    			bindings[i]= new Hashtable();
			}
	    	out = fs.create(outFile);
    	} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}
	
	private static void joinFile(String inputFile) {
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

	private static void joinScan(Scan inscan, String vars, String pat) {
		StringTokenizer vtok = new StringTokenizer(vars);
		int type=0;
		while(vtok.hasMoreTokens()){
			vtok.nextToken();
			type++;
		}
		String joinVars = newjoinVars.split(pat)[1];
		joinVars=joinVars.substring(0, joinVars.indexOf("$$")-1);
		String col=inscan.getInputColumns();
		byte[] rowid =inscan.getStartRow();
		byte[] startr = new byte[1+8+8+2];
		byte[] stopr = new byte[1+8+8+2];
		startr[0] =rowid[0];
		stopr[0] =rowid[0];
		for (int i = 1; i < rowid.length; i++) {
			startr[i] =rowid[i];
			stopr[i] =rowid[i];
		}
		if (rowid.length==17) {
			startr[startr.length-2] =(byte)0;
			startr[startr.length-1] =(byte)0;
			stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		}
		if (rowid.length==9) {
			  for (int i = 9; i < startr.length-2; i++) {
				  startr[i] =(byte)0;
				  stopr[i] =(byte)255;
			  }
			  startr[startr.length-2] =(byte)0;
			  startr[startr.length-1] =(byte)0;
			  stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
			  stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		}
		byte[] bid,a;
		a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}
		Scan scan =new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(70000);
		scan.setCacheBlocks(true);
		scan.addFamily(bid);
		ResultScanner resultScanner=null;
		try {
			resultScanner = table.getScanner(scan);
			Result result = null;
			while((result=resultScanner.next())!=null){
				//System.out.println("size: "+result.size());
				Iterator<KeyValue> it = result.list().iterator();
				while(it.hasNext()){
					KeyValue kv = it.next();
					if(type==1){
						  StringTokenizer vtok2 = new StringTokenizer(vars);
						  String v1=vtok2.nextToken();
						  collect(v1+"#"+Bytes.toLong(kv.getQualifier())+"_", 
								  pat, "");
					}
					else if(type==2){
						  StringTokenizer vtok2 = new StringTokenizer(vars);
						  String v1=vtok2.nextToken();
						  String v2=vtok2.nextToken();
						  byte[] r = kv.getRow();
						  byte[] r1= new byte[8];
						  for (int j = 0; j < r1.length; j++) {
							  r1[j]=r[9+j];
						  }
						  if(joinVars.contains(v1)){
							  collect(v1+"#"+Bytes.toLong(r1)+"!", 
									  pat, 
									  v2+"#"+Bytes.toLong(kv.getQualifier())+"_");
						  }
						  else if(joinVars.contains(v2)){
							  collect(v2+"#"+Bytes.toLong(kv.getQualifier())+"!", 
									  pat, 
									  v1+"#"+Bytes.toLong(r1)+"_");
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


	private static void printJoin(FSDataOutputStream out, int joinid) {
		String key, keyVar, var, value, tpat, else_value ;
		Integer i=0, in_no, bind_no;
		Enumeration varnames = joinVarNames.keys();
		//System.out.println("print");
		while(varnames.hasMoreElements()){
			String sum="J"+joinid+":"+(i+1)+"!";
			var = varnames.nextElement().toString();
			in_no=(Integer)jNo.get(var);
			Enumeration e = bindings[i].keys();
			while(e.hasMoreElements()){
				bind_no=0;
				key = e.nextElement().toString();
				value = (String)bindings[i].get(key);
				//System.out.println("key="+key+" value="+value);
				StringTokenizer t = new StringTokenizer(value);
				tpat=t.nextToken("$");
				if(t.hasMoreTokens()){
					else_value=t.nextToken("$");
				}
				else{
					else_value="";
				}
				StringTokenizer t1 = new StringTokenizer(tpat);
				
				while(t1.hasMoreTokens()){
					tpat=t1.nextToken("_");
					bind_no++;
				}
				if(nonJoinSize==0){//Full input
					//System.out.println("bind_no="+bind_no+" in_no="+in_no);
					if(bind_no==in_no){
						//out.writeBytes(sum+key+"!"+else_value+"\n");
						outKey.set(sum+key+"!"+else_value);
						writeOut(outKey);
					}
				}
				else{

					int vid=getvarind(var);
					int nonjno=nonJoinSizeTab[vid];
					String foundkvals1 =null;
					//System.out.println("bind_no="+bind_no+" in_no="+in_no+" nonjno="+nonjno);
					if(bind_no==in_no-nonjno){
						
						StringTokenizer vt = new StringTokenizer(key.toString());
						String patvals= vt.nextToken("#");
						String keyvals = vt.nextToken("#");
						foundkvals1= patvals+"#";
						String foundkb= "";
						int foundsize=0;
						boolean findDoub=findDouble(vid);
						StringTokenizer tokenizer1 = new StringTokenizer(keyvals);
						while(tokenizer1.hasMoreTokens()) {
							String temp2 = tokenizer1.nextToken("_");
							byte[] temp1=Bytes.toBytes(Long.parseLong(temp2));
							int found=0;
							String outkeytemp="";
							for (int jj = 0; jj < nonjno; jj++) {
								nonJoinCol[vid][jj]=nonJoinCol[vid][jj].replace(":", "");
								
								if(nonJoinCol[vid][jj].contains("?")){
									StringTokenizer tt1 = new StringTokenizer(nonJoinCol[vid][jj]);
									String varname1 = tt1.nextToken("|");
									String varname2 = tt1.nextToken("|");
									byte[] b = new byte[8];
									for (int j = 0; j < 8; j++) {
										b[j]=nonJoinStartRow[vid][jj][j+1];
									}
									if(nonJoinStartRow[vid][jj][0]==(byte)2){//osp
										if(patvals.equals(varname1)){
											//osp
											byte pinakas = (byte)2;
											String outkeytemp1=reduceJoin(pinakas, b, temp1, varname2);
											if(!outkeytemp1.equals("")){
												found++;
												outkeytemp+=outkeytemp1;
											}
										}
										else{
											//pos
											byte pinakas = (byte)3;
											String outkeytemp1=reduceJoin(pinakas,temp1, b,  varname1);
											if(!outkeytemp1.equals("")){
												found++;
												outkeytemp+=outkeytemp1;
											}
												
										}
									}
									else if(nonJoinStartRow[vid][jj][0]==(byte)3){//pos
										if(patvals.equals(varname1)){
											//pos
											byte pinakas = (byte)3;
											String outkeytemp1=reduceJoin(pinakas, b, temp1, varname2);
											if(!outkeytemp1.equals("")){
												found++;
												outkeytemp+=outkeytemp1;
											}
										}
										else{
											//spo
											byte pinakas = (byte)4;
											String outkeytemp1=reduceJoin(pinakas, temp1, b, varname1);
											if(!outkeytemp1.equals("")){
												found++;
												outkeytemp+=outkeytemp1;
											}
										}
									}
									else if(nonJoinStartRow[vid][jj][0]==(byte)4){//spo
										if(patvals.equals(varname1)){//spo
											byte pinakas = (byte)4;
											String outkeytemp1=reduceJoin(pinakas, b, temp1, varname2);
											if(!outkeytemp1.equals("")){
												found++;
												outkeytemp+=outkeytemp1;
											}
										}
										else{
											//osp
											byte pinakas = (byte)2;
											String outkeytemp1=reduceJoin(pinakas, temp1, b, varname1);
											if(!outkeytemp1.equals("")){
												found++;
												outkeytemp+=outkeytemp1;
											}
										}
									}
								}
								else{//have all three values pame panta sto osp
									
									if(nonJoinStartRow[vid][jj][0]==(byte)2){//osp
										byte pinakas = (byte)2;
										byte[] b1 = new byte[8];
										for (int j = 0; j < 8; j++) {
											b1[i]=nonJoinStartRow[vid][jj][i+1];
										}
										byte[] b2 = new byte[8];
										for (int j = 0; j < 8; j++) {
											b2[i]=nonJoinStartRow[vid][jj][i+9];
										}
										found+=reduceJoinAllVar(pinakas, b1, b2, temp1);
									}
									else if(nonJoinStartRow[vid][jj][0]==(byte)3){//pos
										byte[] b1 = new byte[8];
										byte[] b2 = new byte[8];
										byte[] b3 = new byte[8];
										int size =nonJoinStartRow[vid][jj].length;
										byte pinakas=(byte)2;//osp
										for (int j = 0; j < 8; j++) {
											b2[j]= temp1[j];
										}
										for (int i1 = 0; i1 < 8; i1++) {
											b3[i1]=nonJoinStartRow[vid][jj][i1+1];
										}
										//find subclasses
										if(size>17){//uparxoun subclasses
											int ffound = 0 ;
											for (int ik = 0; ik < (size-9)/8; ik++) {
												for (int j = 0; j < 8; j++) {
													b1[j]= nonJoinStartRow[vid][jj][j+9+ik*8];
												}
												//System.out.println(Bytes.toStringBinary(b1));
												ffound+=reduceJoinAllVar(pinakas, b1, b2, b3);
												
											}
											if(ffound>0){
												found++;
											}
										}
										else{//no subclasses
											for (int j = 0; j < 8; j++) {
												b1[j]= nonJoinStartRow[vid][jj][9+j];
											}
											//System.out.println(Bytes.toStringBinary(b1));
											found+=reduceJoinAllVar(pinakas, b1, b2, b3);
										}
										
									}
									else if(nonJoinStartRow[vid][jj][0]==(byte)4){//spo
										byte pinakas = (byte)2;
										byte[] b1 = new byte[8];
										for (int j = 0; j < 8; j++) {
											b1[i]=nonJoinStartRow[vid][jj][i+1];
										}
										byte[] b2 = new byte[8];
										for (int j = 0; j < 8; j++) {
											b2[i]=nonJoinStartRow[vid][jj][i+9];
										}
										found+=reduceJoinAllVar(pinakas, temp1, b1, b2);
									}
								}
								if(found==nonjno){
									break;
								}
							}
							if(found==nonjno){
								if(findDoub){
									String fkvals= temp2+"_";
									outKey.set(sum+foundkvals1+fkvals+"!"+outkeytemp+else_value);
									writeOut(outKey);
								}
								else{
									foundkb+=temp2+"_";
									foundsize++;
								}
								
							}
							
						}
						if((foundsize>0)&& (!findDoub)){
							foundkvals1+= foundkb;
							outKey.set(sum+foundkvals1+"!"+else_value);
							writeOut(outKey);
						}
					}
					
				}
			}
			i++;
		}
		
	}

	private static void collect(String binding, String pat, String else_val) {
		//System.out.println("pat="+pat+" binding="+binding+" else_val="+else_val);
		String tpat, value, el;
		//prepei na kanw binding itterate 

		StringTokenizer t = new StringTokenizer(binding);
		String keyVar = t.nextToken("#");
		Integer index = (Integer)joinVarNames.get(keyVar);
		if(bindings[index].containsKey(binding)){
			value = (String)bindings[index].get(binding);
			StringTokenizer t1 = new StringTokenizer(value);
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
			bindings[index].put(binding, tpat+"$"+el);
		}
		else{
			bindings[index].put(binding, pat+"_$"+else_val);
		}
	}

	private static String breakList(String newline, String binding, String pred) {
		
		StringTokenizer tokenizer = new StringTokenizer(binding);
		while(tokenizer.hasMoreTokens()) {
			String temp1 = tokenizer.nextToken("_");
			newline+=pred+temp1+"_!";
		}
		return newline;
	}

	
	private static int reduceJoinAllVar(byte pinakas, byte[] b1, byte[] b2, byte[] b3) {
		int ret=0;
		byte[] startr= new byte[1+8+8+2];
		byte[] stopr= new byte[1+8+8+2];
		startr[0]=pinakas;
		stopr[0]=pinakas;
		for (int i1 = 0; i1 < 8; i1++) {
			startr[i1+1]=b1[i1];
			stopr[i1+1]=b1[i1];
		}
		for (int i1 = 0; i1 < 8; i1++) {
			startr[i1+9]=b2[i1];
			stopr[i1+9]=b2[i1];
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
	
	
	private static String reduceJoin(byte pinakas, byte[] b1, byte[] b2,
			String varname) {
		String ret="";
		byte[] startr= new byte[1+8+8+2];
		byte[] stopr= new byte[1+8+8+2];
		startr[0]=pinakas;
		stopr[0]=pinakas;
		for (int i1 = 0; i1 < 8; i1++) {
			startr[i1+1]=b1[i1];
			stopr[i1+1]=b1[i1];
		}
		for (int i1 = 0; i1 < 8; i1++) {
			startr[i1+9]=b2[i1];
			stopr[i1+9]=b2[i1];
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
		scan.addColumn(bid);
		scan.setCacheBlocks(true);
		ResultScanner resultScanner=null;
		try {
			resultScanner = table.getScanner(scan);
			Result re;
			while((re = resultScanner.next())!=null){
				if(re.size()!=0){
					KeyValue[] v = re.raw();
					for (int j = 0; j < v.length; j++) {
						ret+=varname+"#"+vtoString(v[j].getQualifier())+"!";
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



	private static void writeOut(Text outKey2) {
		
		if(isLast==1){
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
						pred+="#";
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
    	}
		try {
			out.writeBytes(outKey2+"\n");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	

	private static boolean findDouble(int vid) {
		boolean ret =false;
		int nonjno=nonJoinSizeTab[vid];
		for (int jj = 0; jj < nonjno; jj++) {
			if(nonJoinCol[vid][jj].contains("?")){
				return true;
			}
		}
		return ret;
	}

	private static String transform(String newline, String binding, String pred) {
		
		String bindings="";
		boolean found=false;
		StringTokenizer tokenizer = new StringTokenizer(binding);
		while(tokenizer.hasMoreTokens()) {
			String temp = tokenizer.nextToken("_");
			//System.out.println(temp);
			//System.out.println(binding);
			byte[] temp1=Bytes.toBytes(Long.parseLong(temp));
			byte[] k = new byte[9];
			k[0]=(byte) 1;
			
			for (int j = 0; j < 8; j++) {
				k[j+1]=temp1[j];
			}
			Get get=new Get(k);
			get.addColumn(Bytes.toBytes("A"), Bytes.toBytes("i"));
			try {
				Result result = table.get(get);
				if(!result.isEmpty()){
					bindings+=Bytes.toString(result.raw()[0].getValue())+"_";
					found=true;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		bindings+=" ";
		if(found){
			newline+=pred+bindings;
		}
		else{
			System.out.println("Id not found in names index");
			System.exit(1);
		}
		return newline;
	}
	
	private static String vtoString(byte[] value) {
		  long v = Bytes.toLong(value);
		
		  if(value.length!=8){
			  System.out.println(v);
			  System.exit(1);
		  }
		  return String.valueOf(v)+"_";
		}

	private static int getvarind(String var) {
		for (int i = 0; i < nonJoinVarNames.length; i++) {
			if(var.equals(nonJoinVarNames[i])){
				return i;
			}
		}
		return -1;
	}
}
