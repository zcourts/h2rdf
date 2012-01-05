package translator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import partialJoin.HbaseJoinBGP;

public class Translator {
	public static void executeTranslate(Path outFile, Path inFile) {
		int m_rc;
		String[] args = new String[2];
    	args[0]="output/"+outFile.getName();
    	args[1]="output/"+inFile.getName();
        try {
			m_rc = ToolRunner.run(new Configuration(),new MRTranslate(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
