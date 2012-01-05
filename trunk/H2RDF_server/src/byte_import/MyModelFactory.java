package byte_import;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MyModelFactory {

	private MyModelFactory()
    {}
	
	public static void createBulkModel(String[] path)
    { 
		if(path.length!=1){
			System.out.println("wrong input");
			System.out.println("expected one argument (dataset path in hdfs)");
			return;
		}
		try {
			ToolRunner.run(new Configuration(), new HexastoreBulkImport(), path);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
}
