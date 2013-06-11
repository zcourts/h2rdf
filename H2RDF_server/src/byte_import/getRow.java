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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

public class getRow {

	/**
	 * @param args
	 */
	private static Configuration hconf = HBaseConfiguration.create();
    
	public static void main(String[] args) throws NotSupportedDatatypeException {
		byte[] row = ByteValues.getFullValue("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
		byte[] col=ByteValues.getFullValue("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>");
		//byte[] row = ByteValues.getFullValue("<http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.rdf#kyotoDomainRelevance>");
		//byte[] row = getRowIdbyte("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
		/*getRowIdbyte("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#VisitingProfessor>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Dean>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>");
		getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>");*/
		//byte[] col=getRowIdbyte("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>");
		//byte[] col=ByteValues.getFullValue("<http://www.gate.ac.uk/ns/ontologies/arcomem-data-model.rdf#Term>");
		//byte[] col=ByteValues.getFullValue("\"64.53327\"^^<http://www.w3.org/2001/XMLSchema#double>");
		byte[] startr = new byte[1+2*ByteValues.totalBytes+2];
		byte[] stopr = new byte[1+2*ByteValues.totalBytes+2];
		startr[0]=(byte) 3;
		stopr[0]=(byte) 3;
		for (int i = 0; i < ByteValues.totalBytes; i++) {
			startr[i+1]=row[i];
			stopr[i+1]=row[i];
		}
		for (int i = 0; i < ByteValues.totalBytes; i++) {
			startr[i+ByteValues.totalBytes+1]=col[i];
			stopr[i+ByteValues.totalBytes+1]=col[i];
		}
		startr[1+2*ByteValues.totalBytes]=(byte)0;
		stopr[1+2*ByteValues.totalBytes]=(byte)255;
		startr[1+2*ByteValues.totalBytes+1]=(byte)0;
		stopr[1+2*ByteValues.totalBytes+1]=(byte)255;
		try {
			HTable table=null;
			table = new HTable( hconf, "H2RDF2000" );
			Scan scan =new Scan();
			
			scan.setStartRow(startr);
			scan.setStopRow(stopr);
			Iterator<Result> it = table.getScanner(scan).iterator();
			while(it.hasNext()){
				Result res = it.next();
				if(res.size()>0){
					
					System.out.println("row: "+Bytes.toStringBinary(res.getRow())+" size: "+res.size());
				}
			}
			/*byte[] r = new byte[1+2*ByteValues.totalBytes];
			r[0]=(byte) 3;
			for (int i = 0; i < ByteValues.totalBytes; i++) {
				r[i+1]=row[i];
			}
			for (int i = 0; i < ByteValues.totalBytes; i++) {
				r[i+ByteValues.totalBytes+1]=col[i];
			}
			Get get = new Get(r);
			table = new HTable( hconf, "ARCOMEMDB_stats" );
			Result res = table.get(get);
			if(res.size()>0)
				System.out.println("row: "+Bytes.toStringBinary(r)+" size: "+Bytes.toLong(res.getValue(Bytes.toBytes("size"), Bytes.toBytes(""))));*/
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
