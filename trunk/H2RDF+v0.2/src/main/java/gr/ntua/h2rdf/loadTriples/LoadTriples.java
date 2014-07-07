/*******************************************************************************
 * Copyright 2014 Nikolaos Papailiou
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.loadTriples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class LoadTriples {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		try {
			if(args.length!=2){
				System.out.println("wrong input");
				System.out.println("expected two arguments\n1)dataset path in hdfs\n2)database name");
				return;
			}
			//Create Index
			Configuration conf = new Configuration();
			ToolRunner.run(conf, new DistinctIds(), args);
			//Translate triples and create hexastore index
			conf = new Configuration();
			ToolRunner.run(conf, new TranslateAndImport(), args);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
	}

}
