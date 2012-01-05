package sampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import byte_import.HexastoreBulkImport;
import byte_import.MyModelFactory;

public class SamplerEx {
	public static void main(String[] args)
	 {
		try {
			ToolRunner.run(new Configuration(), new TotalOrderPrep(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }
	
}
