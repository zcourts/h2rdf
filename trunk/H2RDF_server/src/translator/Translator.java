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
package translator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import partialJoin.HbaseJoinBGP;

public class Translator {
	public static void executeTranslate1(Path inFile, Path outFile) {
		int m_rc;
		String[] args = new String[2];
    	args[0]="output/"+inFile.getName();
    	args[1]="output/"+outFile.getName();
        try {
			m_rc = ToolRunner.run(new Configuration(),new MRTranslate1(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void executeTranslate2(Path inFile, Path outFile) {
		int m_rc;
		String[] args = new String[2];
    	args[0]="output/"+inFile.getName();
    	args[1]="output/"+outFile.getName();
        try {
			m_rc = ToolRunner.run(new Configuration(),new MRTranslate2(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
