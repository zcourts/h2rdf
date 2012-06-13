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
package sampler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import byte_import.HexastoreBulkImport;
import byte_import.MyModelFactory;

public class SamplerEx {
	public static void main(String[] args)
	 {
		try {
			if(args.length!=2){
				System.out.println("wrong input");
				System.out.println("expected two arguments\n1)dataset path in hdfs\n2)database name");
				return;
			}
			//sampling
			Configuration conf = new Configuration();
			ToolRunner.run(conf, new TotalOrderPrep(), args);
			//loading
			MyModelFactory.createBulkModel(args) ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }
	
}
