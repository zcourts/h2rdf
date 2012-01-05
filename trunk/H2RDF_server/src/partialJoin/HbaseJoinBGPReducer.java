package partialJoin;

import java.io.IOException;
import java.util.*;

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

import byte_import.MyNewTotalOrderPartitioner;

public class HbaseJoinBGPReducer extends Reducer<Text, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text("");
	private static HBaseConfiguration hconf=new HBaseConfiguration();
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
	private static byte[] SUBCLASS = Bytes.toBytes( new Long("8742859611446415633"));
	private HashSet<Integer> trans_hash=null;
	  

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
		String s1="";
		Set p = new HashSet();
		int i=0;
		if(j==1){
			ValueDoubleMerger.init();
		}
		else{
			ValueMerger.init();
		}
		for(Text value: values) {
			tok=value.toString();
			valueTokenizer = new StringTokenizer(tok);
			temp=valueTokenizer.nextToken("$");
			if(!p.contains(temp)){
				i++;
				p.add(temp);
			}
			if(valueTokenizer.hasMoreTokens()){
				if(j==1){
					ValueDoubleMerger.merge(tok.substring(temp.length()+1), temp);
				}
				else{
					s1+=tok.substring(temp.length()+1);
					//ValueMerger.merge(tok.substring(temp.length()+1), temp);
				}
			}
		}
		/*if(j!=1){
			s1=ValueMerger.getValue();
		}*/
		if(nonJoinSize==0){//Full input
			if(i==jno){


				if(j==1){
					ValueDoubleMerger.itter();
					while(ValueDoubleMerger.hasMore()){
						s1=ValueDoubleMerger.getValue();
						if(!s1.equals("")){
							outKey.set(sum+key+"!"+s1);
							writeOut(outKey, outValue, context);
						}
					}
				}	
				else{
					outKey.set(sum+key+"!"+s1);
					writeOut(outKey, outValue, context);
				}
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
		if(i==jno-nonjno){//exei perasei to map phase join
			
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
				byte[] temp1=Bytes.toBytes(Long.parseLong(temp2));
				int found=0;
				String outkeytemp="";
				for (int jj = 0; jj < nonjno; jj++) {//itterate gia subclasses
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
						//foundkvals+=valueToString(temp1,4);
						outKey.set(sum+foundkvals1+fkvals+"!"+outkeytemp+s1);
						/*Set ret = doubleVar(s1);
						if(ret.isEmpty()){
							s1=removeId(s1);
							outKey.set(sum+s1);
						}
						else{
							s1 = joinDoubleVars(ret, s1);
							outKey.set(sum);
						}*/
						writeOut(outKey, outValue, context);
					}
					else{
						foundkb+=temp2+"_";
						foundsize++;
					}
					
				}
				
			}
			if((foundsize>0)&& (!findDoub)){
				foundkvals1+= foundkb;
				//foundkvals+=valueToString(foundkb,foundsize);
				outKey.set(sum+foundkvals1+"!"+s1);
				/*Set ret = doubleVar(s1);
				if(ret.isEmpty()){
					s1=removeId(s1);
					outKey.set(sum+s1);
				}
				else{
					s1 = joinDoubleVars(ret, s1);
					outKey.set(sum);
				}*/
				writeOut(outKey, outValue, context);
			}
		}
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
		scan.setCaching(256);
		scan.addColumn(Bytes.toBytes("A"), b3);
		ResultScanner resultScanner;
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
		scan.setCaching(256);
		byte[] a, col;
		/*a=Bytes.toBytes("A");
		bid = new byte[a.length];
		for (int i = 0; i < a.length; i++) {
			bid[i]=a[i];
		}*/
		scan.addColumn(Bytes.toBytes("A"), null);
		ResultScanner resultScanner;
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
						Integer id=(int)Long.parseLong(temp);
						trans_hash.add(id);
					}
				}
			}
			outKey2.set(newline);
    	}
		if(isLast==1){// Index Translate
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
    	}
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
		for (int i = 0; i < binding.length; i+=8) {
			byte[] temp1 = new byte[8];
			for (int jj = 0; jj < 8; jj++) {
				temp1[jj]=binding[i+jj];
			}
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
		return newline;
	}

	private boolean findDouble(int vid) {
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
	protected void finalize() throws Throwable {
		if(isLast==2){
			FileSystem fs = FileSystem.get(conf);
			String[] idStr = conf.get("mapred.task.id").split("_");
			short id =Short.parseShort(idStr[idStr.length-2]);
			Path hash_file= new Path("translate/trans_hash_"+conf.get("nikos.inputfile").split("_")[1]+"_"+id);
			if(fs.exists(hash_file)){
				fs.delete(hash_file, true);
			}
			FSDataOutputStream v = fs.create(hash_file);
			Iterator<Integer> it = trans_hash.iterator();
			while(it.hasNext()){
				int n=it.next();
				v.writeUTF(n+"_");
			}
    	}
		super.finalize();
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		conf =context.getConfiguration();
		table = new HTable( hconf, conf.get("nikos.table") );
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream v = fs.open(new Path(conf.get("nikos.inputfile")));
    	joinVars=v.readLine();
    	v.readLine();
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
				scan1.setCaching(254);
				scan1.addFamily(bid);
				try {
					ResultScanner resultScanner = table.getScanner(scan1);
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
    	//printNonJoin();
    	v.close();
    	if(isLast==2){
    		trans_hash = new HashSet<Integer>();
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
	
	private static String vtoString(byte[] value) {
		  long v = Bytes.toLong(value);
		
		  if(value.length!=8){
			  System.out.println(v);
			  System.exit(1);
		  }
		  return String.valueOf(v)+"_";
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