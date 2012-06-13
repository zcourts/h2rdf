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
package byte_import;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class MyModelFactory {

	private MyModelFactory()
    {}
	
	public static void createBulkModel(String[] path)
    { 
		if(path.length!=2){
			System.out.println("wrong input");
			System.out.println("expected two arguments\n1)dataset path in hdfs\n2)database name");
			return;
		}
		try {
			ToolRunner.run(new Configuration(), new HexastoreBulkImport(), path);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
}
