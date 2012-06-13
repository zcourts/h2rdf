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
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

public class ThreadedProcessor extends Thread{
	private int threadId, size, in_no;
	private final int totsize = ByteValues.totalBytes, nonJoinSize=SerialQueryProcessorThreaded.nonJoinSize;
	private String[] keyTable;
	private String var, sum;
	private Hashtable<String, ValueDoubleMerger> bindings;
	
	public ThreadedProcessor(int threadId, int size, String[] keyTable, Hashtable<String, ValueDoubleMerger> bindings2, int in_no, String var, String sum) {
		super("ThreadedProcessor");
		this.threadId =threadId;
		this.size = size;
		this.keyTable = keyTable;
		this.in_no = in_no;
		this.var = var;
		this.sum = sum;
		this.bindings = bindings2;
	}
	
	public void run() {
		String key, tpat, else_value;
		ValueDoubleMerger value;
		Text outKey= new Text();
		try {
		int end = (threadId+1)*size;
		if(end>keyTable.length)
			end =keyTable.length;
		
		
		for (int i = threadId*size; i <end; i++) {
			
			key = keyTable[i];
			value = bindings.get(key);
			int bind_no=value.getTotalPatterns();
			
			
			if(nonJoinSize==0){//Full input
				//System.out.println("bind_no="+bind_no+" in_no="+in_no);
				if(bind_no==in_no){
					//out.writeBytes(sum+key+"!"+else_value+"\n");
					if(value.itter()){
						while(value.hasMore()){
							String s1=value.getValue();
							if(!s1.equals("")){
								outKey.set(sum+key+"!"+value.getTotal()+"!"+s1);
								SerialQueryProcessorThreaded.writeOut(outKey);
							}
						}
					}
					else{
						outKey.set(sum+key+"!"+value.getTotal()+"!");
							SerialQueryProcessorThreaded.writeOut(outKey);
					}
					
					//outKey.set(sum+key+"!"+else_value+"!");
					//SerialQueryProcessorThreaded.writeOut(outKey);
				}
			}
			else {

				int vid=SerialQueryProcessorThreaded.getvarind(var);
				int nonjno=SerialQueryProcessorThreaded.nonJoinSizeTab[vid];
				//System.out.println(vid);
				//System.out.println(nonjno);
				String foundkvals1 =null;
				//System.out.println("bind_no="+bind_no+" in_no="+in_no+" nonjno="+nonjno);
				if(bind_no==in_no-nonjno){
					StringTokenizer vt = new StringTokenizer(key.toString());
					String patvals= vt.nextToken("#");
					String keyvals = vt.nextToken("#");
					foundkvals1= patvals+"#";
					String foundkb= "";
					int foundsize=0;
					boolean findDoub=SerialQueryProcessorThreaded.findDouble(vid);
					StringTokenizer tokenizer1 = new StringTokenizer(keyvals);
					while(tokenizer1.hasMoreTokens()) {
						String temp2 = tokenizer1.nextToken("_");
						//System.out.println(temp2);
						byte[] temp3=Bytes.toBytes(Long.parseLong(temp2.substring(temp2.indexOf("|")+1)));
						byte[] temp1=new byte[totsize];
						temp1[0]=(byte) new Byte(temp2.substring(0,temp2.indexOf("|")));
						for (int j = 0; j < 8; j++) {
							temp1[j+1]=temp3[j];
						}
						
						int found=0;
						String outkeytemp="";
						for (int jj = 0; jj < nonjno; jj++) {
							SerialQueryProcessorThreaded.nonJoinCol[vid][jj]=SerialQueryProcessorThreaded.nonJoinCol[vid][jj].replace(":", "");
							//System.out.println(nonJoinCol[vid][jj]);
							if(SerialQueryProcessorThreaded.nonJoinCol[vid][jj].contains("?")){
								StringTokenizer tt1 = new StringTokenizer(SerialQueryProcessorThreaded.nonJoinCol[vid][jj]);
								String varname1 = tt1.nextToken("|");
								String varname2 = tt1.nextToken("|");
								//System.out.println(varname1+" "+varname2);
								byte[] b = new byte[totsize];
								for (int j = 0; j < totsize; j++) {
									b[j]=SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][j+1];
								}
								if(SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][0]==(byte)2){//osp
									if(patvals.equals(varname1)){
										//osp
										byte pinakas = (byte)2;
										String outkeytemp1=SerialQueryProcessorThreaded.reduceJoin(pinakas, b, temp1, varname2);
										value.merge(outkeytemp1, "K"+jj);
										if(!outkeytemp1.equals("")){
											found++;
											outkeytemp+=outkeytemp1;
										}
									}
									else{
										//pos
										byte pinakas = (byte)3;
										String outkeytemp1=SerialQueryProcessorThreaded.reduceJoin(pinakas,temp1, b,  varname1);
										value.merge(outkeytemp1, "K"+jj);
										if(!outkeytemp1.equals("")){
											found++;
											outkeytemp+=outkeytemp1;
										}
											
									}
								}
								else if(SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][0]==(byte)3){//pos
									if(patvals.equals(varname1)){
										//pos
										byte pinakas = (byte)3;
										String outkeytemp1=SerialQueryProcessorThreaded.reduceJoin(pinakas, b, temp1, varname2);
										value.merge(outkeytemp1, "K"+jj);
										if(!outkeytemp1.equals("")){
											found++;
											outkeytemp+=outkeytemp1;
										}
									}
									else{
										//spo
										byte pinakas = (byte)4;
										String outkeytemp1=SerialQueryProcessorThreaded.reduceJoin(pinakas, temp1, b, varname1);
										value.merge(outkeytemp1, "K"+jj);
										if(!outkeytemp1.equals("")){
											found++;
											outkeytemp+=outkeytemp1;
										}
									}
								}
								else if(SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][0]==(byte)4){//spo
									if(patvals.equals(varname1)){//spo
										byte pinakas = (byte)4;
										String outkeytemp1=SerialQueryProcessorThreaded.reduceJoin(pinakas, b, temp1, varname2);
										value.merge(outkeytemp1, "K"+jj);
										if(!outkeytemp1.equals("")){
											found++;
											outkeytemp+=outkeytemp1;
										}
									}
									else{
										//osp
										byte pinakas = (byte)2;
										String outkeytemp1=SerialQueryProcessorThreaded.reduceJoin(pinakas, temp1, b, varname1);
										value.merge(outkeytemp1, "K"+jj);
										if(!outkeytemp1.equals("")){
											found++;
											outkeytemp+=outkeytemp1;
										}
									}
								}
							}
							else{//have all three values pame panta sto osp
								
								if(SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][0]==(byte)2){//osp
									byte pinakas = (byte)2;
									byte[] b1 = new byte[totsize];
									for (int j = 0; j < totsize; j++) {
										b1[j]=SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][j+1];
									}
									byte[] b2 = new byte[totsize];
									for (int j = 0; j < totsize; j++) {
										b2[j]=SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][j+totsize+1];
									}
									found+=SerialQueryProcessorThreaded.reduceJoinAllVar(pinakas, b1, b2, temp1);
								}
								else if(SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][0]==(byte)3){//pos
									byte[] b1 = new byte[totsize];
									byte[] b2 = new byte[totsize];
									byte[] b3 = new byte[totsize];
									int size =SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj].length;
									byte pinakas=(byte)2;//osp
									for (int j = 0; j < totsize; j++) {
										b2[j]= temp1[j];
									}
									for (int i1 = 0; i1 < totsize; i1++) {
										b3[i1]=SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][i1+1];
									}
									//find subclasses
									if(size>SerialQueryProcessorThreaded.rowlength){//uparxoun subclasses
										int ffound = 0 ;
										for (int ik = 0; ik < (size-totsize-1)/totsize; ik++) {
											for (int j = 0; j < totsize; j++) {
												b1[j]= SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][j+totsize+1+ik*totsize];
											}
											//System.out.println(Bytes.toStringBinary(b1));
											ffound+=SerialQueryProcessorThreaded.reduceJoinAllVar(pinakas, b1, b2, b3);
											
										}
										if(ffound>0){
											found++;
										}
									}
									else{//no subclasses
										for (int j = 0; j < totsize; j++) {
											b1[j]= SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][totsize+1+j];
										}
										//System.out.println(Bytes.toStringBinary(b1));
										found+=SerialQueryProcessorThreaded.reduceJoinAllVar(pinakas, b1, b2, b3);
									}
									
								}
								else if(SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][0]==(byte)4){//spo
									byte pinakas = (byte)2;
									byte[] b1 = new byte[totsize];
									for (int j = 0; j < totsize; j++) {
										b1[j]=SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][j+1];
									}
									byte[] b2 = new byte[totsize];
									for (int j = 0; j < totsize; j++) {
										b2[j]=SerialQueryProcessorThreaded.nonJoinStartRow[vid][jj][j+totsize+1];
									}
									found+=SerialQueryProcessorThreaded.reduceJoinAllVar(pinakas, temp1, b1, b2);
								}
							}
							if(found==nonjno){
								break;
							}
						}
						if(found==nonjno){
							if(findDoub){
								String fkvals= temp2+"_";
								if(value.itter()){
									while(value.hasMore()){
										String s1=value.getValue();
										if(!s1.equals("")){
											outKey.set(sum+foundkvals1+fkvals+"!"+value.getTotal()+"!"+s1);
											SerialQueryProcessorThreaded.writeOut(outKey);
										}
									}
								}
								else{
									outKey.set(sum+sum+foundkvals1+fkvals+"!"+value.getTotal()+"!");
										SerialQueryProcessorThreaded.writeOut(outKey);
								}
								
								//outKey.set(sum+foundkvals1+fkvals+"!"+outkeytemp+else_value+"!");
								//SerialQueryProcessorThreaded.writeOut(outKey);
							}
							else{
								foundkb+=temp2+"_";
								foundsize++;
							}
							
						}
						
					}
					if((foundsize>0)&& (!findDoub)){
						foundkvals1+= foundkb;
						if(value.itter()){
							while(value.hasMore()){
								String s1=value.getValue();
								if(!s1.equals("")){
									outKey.set(sum+foundkvals1+"!"+value.getTotal()+"!"+s1);
									SerialQueryProcessorThreaded.writeOut(outKey);
								}
							}
						}
						else{
							outKey.set(sum+foundkvals1+"!"+value.getTotal()+"!");
								SerialQueryProcessorThreaded.writeOut(outKey);
						}
						
					}
				}
				
			}
		
		}
			} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotSupportedDatatypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
