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
import java.util.*;

import javaewah.EWAHCompressedBitmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import translator.CastLongToInt;

import byte_import.MyNewTotalOrderPartitioner;
import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

public class HbaseJoinBGPReducer extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text("");
	private static Configuration hconf= HBaseConfiguration.create();
	private static HTable table;
	private String joinVars;
	private String joinNo;
	private String resultVars;
	private String[] nonJoinVarNames;
	private String[][] nonJoinCol;
	private byte[][][] nonJoinStartRow;
	private int[] nonJoinSizeTab;
	private int j, nonJoinSize;
	private int isLast;
	private Configuration conf;
	private static byte[] SUBCLASS ;
	private SortedSet<Integer> trans_hash=null;
	private static final int totsize= ByteValues.totalBytes, rowlength=1+2*totsize;
	  

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
		StringTokenizer keyTokenizer = new StringTokenizer(key.toString());
		String jv=keyTokenizer.nextToken("#");
		
    	StringTokenizer jtok = new StringTokenizer(joinVars);
    	StringTokenizer jnotok = new StringTokenizer(joinNo);
		int jno=0;
		int fileno =0;
		while (jtok.hasMoreTokens()) {
			fileno++;
			if(jv.equals(jtok.nextToken())){
				jno=Integer.parseInt(jnotok.nextToken());
				break;
			}
			else
				jnotok.nextToken();
		}
		
		StringTokenizer valueTokenizer;
		String sum = "J"+j+":"+fileno+"!", temp, tok;
		Set p = new HashSet();

		//System.out.println("key: "+ key.toString());
		ValueDoubleMerger merger = new ValueDoubleMerger(conf);
		
		for(Text value: values) {
			tok=value.toString();
			valueTokenizer = new StringTokenizer(tok);
			temp=valueTokenizer.nextToken("$");
			//System.out.println("pat: "+temp + "key: "+ key.toString());
			if(valueTokenizer.hasMoreTokens()){
				merger.merge(tok.substring(temp.length()+1), temp);
			}
			else{
				merger.merge("", temp);
			}
		}

		int bind_no=merger.getTotalPatterns();
		//System.out.println("bind_no:"+bind_no);
		//System.out.println("jno:"+jno);
		if(nonJoinSize==0){//Full input
			if(bind_no==jno){
				if(merger.itter()){
					while(merger.hasMore()){
						String s1=merger.getValue();
						if(!s1.equals("")){
							outKey.set(sum+key+"!"+merger.getTotal()+"!"+s1);
							writeOut(outKey, outValue, context);
						}
					}
				}
				else{
					outKey.set(sum+key+"!"+merger.getTotal()+"!");
					writeOut(outKey, outValue, context);
				}
				
				
				/*if(j==1){
					ValueDoubleMerger.itter();
					while(ValueDoubleMerger.hasMore()){
						s1=ValueDoubleMerger.getValue();
						if(!s1.equals("")){
							outKey.set(sum+key+"!"+s1);
							writeOut(outKey, outValue, context);
						}
					}
					//outKey.set(sum+key+"!"+s1);
					//writeOut(outKey, outValue, context);
				}	
				else{
					outKey.set(sum+key+"!"+s1);
					writeOut(outKey, outValue, context);
				}*/
				/*Set ret = doubleVar(s1);
				if(ret.isEmpty()){
					s1=removeId(s1);
					outKey.set(sum+s1);
				}
				else{
					s1 = joinDoubleVars(ret, s1);
					outKey.set(sum);
				}*/
			}
			return;
		}
		int vid=getvarind(jv);
		
		int nonjno=nonJoinSizeTab[vid];
		String foundkvals1 =null;
		if(bind_no==jno-nonjno){//exei perasei to map phase join
			
			StringTokenizer vt = new StringTokenizer(key.toString());
			String patvals= vt.nextToken("#");
			String keyvals = vt.nextToken("#");
			foundkvals1= patvals+"#";
			String foundkb= "";
			int foundsize=0;
			boolean findDoub=findDouble(vid);
			StringTokenizer tokenizer1 = new StringTokenizer(keyvals);
			while(tokenizer1.hasMoreTokens()) {//itterate sta binding tou kleidiou isws den xreiazetai
				String temp2 = tokenizer1.nextToken("_");
				byte[] temp3=Bytes.toBytes(Long.parseLong(temp2.substring(temp2.indexOf("|")+1)));
				byte[] temp1=new byte[totsize];
				temp1[0]=(byte) new Byte(temp2.substring(0,temp2.indexOf("|")));
				for (int j = 0; j < 8; j++) {
					temp1[j+1]=temp3[j];
				}
				int found=0;
				String outkeytemp="";
				for (int jj = 0; jj < nonjno; jj++) {//itterate gia subclasses
					nonJoinCol[vid][jj]=nonJoinCol[vid][jj].replace(":", "");
					
					if(nonJoinCol[vid][jj].contains("?")){
						StringTokenizer tt1 = new StringTokenizer(nonJoinCol[vid][jj]);
						String varname1 = tt1.nextToken("|");
						String varname2 = tt1.nextToken("|");
						byte[] b = new byte[totsize];
						for (int j = 0; j < totsize; j++) {
							b[j]=nonJoinStartRow[vid][jj][j+1];
						}
						if(nonJoinStartRow[vid][jj][0]==(byte)2){//osp
							if(patvals.equals(varname1)){
								//osp
								byte pinakas = (byte)2;
								String outkeytemp1=reduceJoin(pinakas, b, temp1, varname2);
								merger.merge(outkeytemp1, "K"+jj);
								if(!outkeytemp1.equals("")){
									found++;
									outkeytemp+=outkeytemp1;
								}
							}
							else{
								//pos
								byte pinakas = (byte)3;
								String outkeytemp1=reduceJoin(pinakas,temp1, b,  varname1);
								merger.merge(outkeytemp1, "K"+jj);
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
								merger.merge(outkeytemp1, "K"+jj);
								if(!outkeytemp1.equals("")){
									found++;
									outkeytemp+=outkeytemp1;
								}
							}
							else{
								//spo
								byte pinakas = (byte)4;
								String outkeytemp1=reduceJoin(pinakas, temp1, b, varname1);
								merger.merge(outkeytemp1, "K"+jj);
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
								merger.merge(outkeytemp1, "K"+jj);
								if(!outkeytemp1.equals("")){
									found++;
									outkeytemp+=outkeytemp1;
								}
							}
							else{
								//osp
								byte pinakas = (byte)2;
								String outkeytemp1=reduceJoin(pinakas, temp1, b, varname1);
								merger.merge(outkeytemp1, "K"+jj);
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
							byte[] b1 = new byte[totsize];
							for (int j = 0; j < totsize; j++) {
								b1[j]=nonJoinStartRow[vid][jj][j+1];
							}
							byte[] b2 = new byte[totsize];
							for (int j = 0; j < totsize; j++) {
								b2[j]=nonJoinStartRow[vid][jj][j+1+totsize];
							}
							found+=reduceJoinAllVar(pinakas, b1, b2, temp1);
						}
						else if(nonJoinStartRow[vid][jj][0]==(byte)3){//pos
							byte[] b1 = new byte[totsize];
							byte[] b2 = new byte[totsize];
							byte[] b3 = new byte[totsize];
							int size =nonJoinStartRow[vid][jj].length;
							byte pinakas=(byte)2;//osp
							for (int j = 0; j < totsize; j++) {
								b2[j]= temp1[j];
							}
							for (int i1 = 0; i1 < totsize; i1++) {
								b3[i1]=nonJoinStartRow[vid][jj][i1+1];
							}
							//find subclasses
							if(size>rowlength){//uparxoun subclasses
								int ffound = 0 ;
								for (int ik = 0; ik < (size-1-totsize)/totsize; ik++) {
									for (int j = 0; j < totsize; j++) {
										b1[j]= nonJoinStartRow[vid][jj][j+1+totsize+ik*totsize];
									}
									//System.out.println(Bytes.toStringBinary(b1));
									ffound+=reduceJoinAllVar(pinakas, b1, b2, b3);
									
								}
								if(ffound>0){
									found++;
								}
							}
							else{//no subclasses
								for (int j = 0; j < totsize; j++) {
									b1[j]= nonJoinStartRow[vid][jj][1+totsize+j];
								}
							//System.out.println(Bytes.toStringBinary(b1));
								found+=reduceJoinAllVar(pinakas, b1, b2, b3);
							}
							
						}
						else if(nonJoinStartRow[vid][jj][0]==(byte)4){//spo
							byte pinakas = (byte)2;
							byte[] b1 = new byte[totsize];
							for (int j = 0; j < totsize; j++) {
								b1[j]=nonJoinStartRow[vid][jj][j+1];
							}
							byte[] b2 = new byte[totsize];
							for (int j = 0; j < totsize; j++) {
								b2[j]=nonJoinStartRow[vid][jj][j+1+totsize];
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
						//foundkvals+=valueToString(temp1,4);

						if(merger.itter()){
							while(merger.hasMore()){
								String s1=merger.getValue();
								if(!s1.equals("")){
									outKey.set(sum+foundkvals1+fkvals+"!"+merger.getTotal()+"!"+s1);
									writeOut(outKey, outValue, context);
								}
							}
						}
						else{
							outKey.set(sum+sum+foundkvals1+fkvals+"!"+merger.getTotal()+"!");
							writeOut(outKey, outValue, context);
						}
						//outKey.set(sum+foundkvals1+fkvals+"!"+outkeytemp+s1);
						/*Set ret = doubleVar(s1);
						if(ret.isEmpty()){
							s1=removeId(s1);
							outKey.set(sum+s1);
						}
						else{
							s1 = joinDoubleVars(ret, s1);
							outKey.set(sum);
						}*/
						//writeOut(outKey, outValue, context);
					}
					else{
						foundkb+=temp2+"_";
						foundsize++;
					}
					
				}
				
			}
			if((foundsize>0)&& (!findDoub)){
				foundkvals1+= foundkb;
				if(merger.itter()){
					while(merger.hasMore()){
						String s1=merger.getValue();
						if(!s1.equals("")){
							outKey.set(sum+foundkvals1+"!"+merger.getTotal()+"!"+s1);
							writeOut(outKey, outValue, context);
						}
					}
				}
				else{
					outKey.set(sum+foundkvals1+"!"+merger.getTotal()+"!");
					writeOut(outKey, outValue, context);
				}
				//foundkvals+=valueToString(foundkb,foundsize);
				//outKey.set(sum+foundkvals1+"!"+s1);
				/*Set ret = doubleVar(s1);
				if(ret.isEmpty()){
					s1=removeId(s1);
					outKey.set(sum+s1);
				}
				else{
					s1 = joinDoubleVars(ret, s1);
					outKey.set(sum);
				}*/
				//writeOut(outKey, outValue, context);
			}
		}
 	}

	private static int reduceJoinAllVar(byte pinakas, byte[] b1, byte[] b2, byte[] b3) {
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
			startr[i1+1+totsize]=b2[i1];
			stopr[i1+1+totsize]=b2[i1];
		}
		startr[startr.length-2] =(byte)0;
		startr[startr.length-1] =(byte)0;
		stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		Scan scan = new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(256);
		scan.addColumn(Bytes.toBytes("A"), b3);
		//System.out.println("scan"+ Bytes.toStringBinary(startr));
		ResultScanner resultScanner;
		try {
			resultScanner = table.getScanner(scan);
			Result re;
			while((re = resultScanner.next())!=null){
				if(re.size()!=0){
					//System.out.println("found");
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
		}
		return ret;
	}
	
	
	private static String reduceJoin(byte pinakas, byte[] b1, byte[] b2,
			String varname) {
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
			startr[i1+1+totsize]=b2[i1];
			stopr[i1+1+totsize]=b2[i1];
		}
		startr[startr.length-2] =(byte)0;
		startr[startr.length-1] =(byte)0;
		stopr[stopr.length-2] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		stopr[stopr.length-1] =(byte)MyNewTotalOrderPartitioner.MAX_HBASE_BUCKETS;
		Scan scan = new Scan();
		scan.setStartRow(startr);
		scan.setStopRow(stopr);
		scan.setCaching(256);
		byte[] a, col;
		/*a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}*/
		scan.addFamily(Bytes.toBytes("A"));
		ResultScanner resultScanner;
		//System.out.println("scan"+ Bytes.toStringBinary(startr) +" stop:"+Bytes.toStringBinary(stopr));
		try {
			resultScanner = table.getScanner(scan);
			Result re;
			while((re = resultScanner.next())!=null){
				if(re.size()!=0){
					//System.out.println("found");
					KeyValue[] v = re.raw();
					for (int j = 0; j < v.length; j++) {
						ret+=varname+"#"+vtoString(v[j].getQualifier())+"!";
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		return ret;
	}



	private void writeOut(Text outKey2, Text outValue2, Context context) {
		
		if(isLast==2){// prepare data for MapReduce translation
    		StringTokenizer list;
    		list=new StringTokenizer(outKey2.toString());
    		list.nextToken("!");
			StringTokenizer tok;
			String newline = "";
			while (list.hasMoreTokens()) {
				String binding=list.nextToken("!");
				tok=new StringTokenizer(binding);
				String pred=tok.nextToken("#");
				if(resultVars.contains(pred.substring(1))){
					newline+=binding+"!";
					pred+="#";
					if(!tok.hasMoreTokens()){
						System.exit(2);
					}
					String b = tok.nextToken("#");
					StringTokenizer tokenizer = new StringTokenizer(b);
					while(tokenizer.hasMoreTokens()) {
						String temp = tokenizer.nextToken("_");
						Integer id=CastLongToInt.castLong(Long.parseLong(temp));
						trans_hash.add(id);
					}
				}
			}
			outKey2.set(newline);
    	}
		/*if(isLast==1){// Index Translate
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
							System.exit(2);
						}
						//byte[] b = Bytes.toBytes(tok.nextToken("#").toCharArray());
						String b = tok.nextToken("#");
						newline=transform(newline, b, pred);
					}
				//}
			}
			outKey2.set(newline);
    	}*/
		try {
			context.write(outKey2, outValue2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String transformBytes(String newline, byte[] binding, String pred) {
		
		String bindings="";
		boolean found=false;
		for (int i = 0; i < binding.length; i+=totsize) {
			byte[] temp1 = new byte[totsize];
			for (int jj = 0; jj < totsize; jj++) {
				temp1[jj]=binding[i+jj];
			}
			byte[] k = new byte[1+totsize];
			k[0]=(byte) 1;
			
			for (int j = 0; j < totsize; j++) {
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
		return newline;
	}

	private boolean findDouble(int vid) {
		boolean ret =false;
		int nonjno=nonJoinSizeTab[vid];
		for (int jj = 0; jj < nonjno; jj++) {
			if(nonJoinCol[vid][jj]!=null){
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
			byte[] k = new byte[1+totsize];
			k[0]=(byte) 1;
			
			for (int j = 0; j < totsize; j++) {
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

	private boolean contains(byte[] v, byte[] qual) {
		boolean ret =false;
		for (int i = 0; i < v.length; i+=4) {
			int k=0;
			for (int j = 0; j < 4; j++) {
				if(v[i+j]==qual[j])
					k++;
			}
			if(k==4){
				ret = true;
				break;
			}
		}
		return ret;
	}



	private static String valueToString(byte[] value, int foundsize) {
		String ret ="";
		for (int i = 0; i < foundsize; i+=4) {
			byte[] temp = new byte[4];
			for (int j = 0; j < 4; j++) {
				temp[j]=value[i+j];
			}
			ret+=Bytes.toInt(temp)+"_";
		}
		
		return ret;
	}

	private String joinDoubleVars(Set ret, String sum) {
		
		return "no";
	}


	private Set doubleVar(String tok) {
		Set idSet = new HashSet();
		Set varSet = new HashSet();
		Set ret = new HashSet();
		StringTokenizer tokenizer = new StringTokenizer(tok);
		while(tokenizer.hasMoreTokens()){
			String temp=tokenizer.nextToken();
			StringTokenizer t = new StringTokenizer(temp);
			String id=t.nextToken("#");
			if(t.hasMoreTokens())
				id+="#"+t.nextToken("#");
			if(!idSet.contains(id)){
				idSet.add(id);
			}
			System.out.println(id);
		}

		Iterator it = idSet.iterator();
		while (it.hasNext()) {
			String id = (String) it.next();
			StringTokenizer t = new StringTokenizer(id);
			String idt = t.nextToken("#");
			String var="";
			if(t.hasMoreTokens()){
				var=t.nextToken("#");
				if(!varSet.contains(var)){
					varSet.add(id);
				}
				else{
					ret.add(idt+"#"+var);
					System.out.println(ret.isEmpty());
					System.exit(1);
				}
			}
		}
		return ret;
	}


	private int getInd(String temp, String[] varNames) {
		int ret=-1;
		for (int i = 0; i < varNames.length; i++) {
			if(temp.equals(varNames[i])){
				ret=i;
				break;
			}
		}
		return ret;
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		if(isLast==2){
			FileSystem fs = FileSystem.get(conf);
			String[] idStr = conf.get("mapred.task.id").split("_");
			short id =Short.parseShort(idStr[idStr.length-2]);
			Path hash_file= new Path("translate/trans_hash_"+conf.get("nikos.inputfile").split("_")[1]+"/"+id);
			if(fs.exists(hash_file)){
				fs.delete(hash_file, true);
			}
			FSDataOutputStream v = fs.create(hash_file);
			Iterator<Integer> it = trans_hash.iterator();
			EWAHCompressedBitmap ewahBitmap = new EWAHCompressedBitmap();
			while(it.hasNext()){
				int n= it.next();
				
		        ewahBitmap.set(n);
		        
				//v.writeUTF(n+"_");
			}
			ewahBitmap.serialize(v);
			v.flush();
			v.close();
			//v.writeUTF("Bitmap size in bytes: "+ewahBitmap.sizeInBytes());
    	}
		super.cleanup(context);
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			  SUBCLASS = ByteValues.getFullValue("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
		} catch (NotSupportedDatatypeException e) {
			  throw new IOException("Not supported datatype");
		}
		conf =context.getConfiguration();
		//System.out.println("table: "+conf.get("nikos.table"));
		table = new HTable( hconf, conf.get("nikos.table") );
    	joinVars=conf.get("input.joinvars");
    	joinNo=conf.get("input.retno");
    	j=Integer.parseInt(conf.get("input.joins"));
    	isLast=Integer.parseInt(conf.get("input.last"));
    	resultVars=conf.get("input.resultVars");
    	
    	String temp=null;
    	nonJoinVarNames = new String[5];
    	nonJoinCol = new String[5][10];
    	nonJoinStartRow = new byte[5][10][];
    	nonJoinSize=0;
    	nonJoinSizeTab=new int[5];
    	for (int i = 0; i < nonJoinSizeTab.length; i++) {
        	nonJoinSizeTab[i]=0;
		}
    	Integer.parseInt(conf.get("input.reduceScans"));
    	
    	
    	for (int i = 0; i < Integer.parseInt(conf.get("input.reduceScans")); i++) {
    		temp=conf.get("input.reduceScans."+i+".fname");
    		System.out.println(temp );
    		int id=getvarind(temp);
    		if(id==-1){
    			nonJoinVarNames[nonJoinSize]=temp;
    			id=nonJoinSize;
    			nonJoinSize++;
    		}
    		System.out.println(id );
    		byte[] rowid= Bytes.toBytesBinary(conf.get("input.reduceScans."+i+".startrow"));
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
			//System.out.println(conf.get("input.reduceScans."+i+".columns"));
    		nonJoinCol[id][nonJoinSizeTab[id]]=conf.get("input.reduceScans."+i+".columns");
    		if(nonJoinCol[id][nonJoinSizeTab[id]]==null)
    			nonJoinCol[id][nonJoinSizeTab[id]]="";
    		nonJoinSizeTab[id]++;
    	
    		/*nonJoinStartRow[id][nonJoinSizeTab[id]] = new byte[rowid.length];
			for (int i2 = 0; i2 < rowid.length; i2++) {
				nonJoinStartRow[id][nonJoinSizeTab[id]][i2]=rowid[i2];
			}
    		nonJoinCol[id][nonJoinSizeTab[id]]=joinConf.get("input.reduceScans."+i+".columns");
    		nonJoinSizeTab[id]++;*/
		}
    	//printNonJoin();
    	if(isLast==2){
    		trans_hash = new TreeSet<Integer>();
    	}
	}


	private void printNonJoin() {
    	for (int i = 0; i < nonJoinSize; i++) {
    		System.out.println(nonJoinVarNames[i]);
    		for (int ii = 0; ii < nonJoinSizeTab[i]; ii++) {
        		System.out.println(Bytes.toStringBinary(nonJoinStartRow[i][ii]));
        		System.out.println(nonJoinCol[i][ii]);
    		}
		}
	}
	
	private static String vtoString(byte[] value) throws IOException {
		  try {
			  //System.out.println(ByteValues.getStringValue(value));
			return ByteValues.getStringValue(value)+"_";
		  } catch (NotSupportedDatatypeException e) {
			  throw new IOException("Not supported datatype");
		  }
	}

	private int getvarind(String var) {
		for (int i = 0; i < nonJoinVarNames.length; i++) {
			if(var.equals(nonJoinVarNames[i])){
				return i;
			}
		}
		return -1;
	}

	
}
