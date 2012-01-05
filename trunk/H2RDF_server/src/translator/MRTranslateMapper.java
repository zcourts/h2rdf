package translator;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRTranslateMapper extends Mapper<ImmutableBytesWritable, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	private String jo;
	private String newjoinVars;
	
	public void map(ImmutableBytesWritable key, Text value, Context context) throws IOException {
		
    	
		String line = value.toString();
		if(!(line.startsWith("P") || line.startsWith("J"))){
			return;
		}
		StringTokenizer tokenizer = new StringTokenizer(line);
		String pat = tokenizer.nextToken("!");
		String joinVars = newjoinVars.split(pat)[1];
		joinVars=joinVars.substring(0, joinVars.indexOf("$$")-1);
		StringTokenizer tok;
		String newline="";
		String else_value=pat+"$";
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
			else_value=pat+"$";
		
		tokenizer = new StringTokenizer(newline);
		while (tokenizer.hasMoreTokens()) {
			String binding = tokenizer.nextToken("!");
			tok=new StringTokenizer(binding);
			String jvar=tok.nextToken("#");
			if(joinVars.contains(jvar)){
				//join variable
				outKey.set(binding);
				outValue.set(else_value);
				try {
					context.write(outKey, outValue);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
    	FileSystem fs = FileSystem.get(conf);
    	FSDataInputStream v = fs.open(new Path(conf.get("nikos.inputfile")));
    	jo=v.readLine();
    	newjoinVars=v.readLine();
    	v.close();
	}
	private String breakList(String newline, String binding, String pred) {

		StringTokenizer tokenizer = new StringTokenizer(binding);
		while(tokenizer.hasMoreTokens()) {
			String temp1 = tokenizer.nextToken("_");
			newline+=pred+temp1+"_!";
		}
		return newline;
	}

	
}

