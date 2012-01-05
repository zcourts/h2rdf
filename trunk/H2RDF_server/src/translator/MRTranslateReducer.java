package translator;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MRTranslateReducer extends Reducer<Text, Text, Text, Text> {
	  

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
		StringTokenizer keyTokenizer = new StringTokenizer(key.toString());
		String jv=keyTokenizer.nextToken("#");
 	}

	
}
