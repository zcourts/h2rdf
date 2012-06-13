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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class decode {
	
	private static HBaseConfiguration hconf=new HBaseConfiguration();
	
	public static void main(String[] args) {
		Integer id = Integer.parseInt(args[0]);
		System.out.println(id);
		byte[] k = new byte[5];
		k[0]=(byte) 1;
		byte[] col=getColIdbyte(id);
		
		for (int i = 0; i < 4; i++) {
			k[i+1]=col[i];
		}
		System.out.println(Bytes.toStringBinary(k));
		
		Get get=new Get(k);
		//get.addColumn(Bytes.toBytes("A:i"));
		HTable table;
		try {
			table = new HTable( hconf, "new" );
			Result result = table.get(get);
			KeyValue[] g = result.raw();
			System.out.println("dfgsdfgsdfgsdfgsdfffffffffffffffffffffffffffffffffff");
			System.out.println("dfgsdfgsdfgsdfgsdfffffffffffffffffffffffffffffffffff");
			System.out.println("dfgsdfgsdfgsdfgsdfffffffffffffffffffffffffffffffffff");
			System.out.println("dfgsdfgsdfgsdfgsdfffffffffffffffffffffffffffffffffff");
			System.out.println("dfgsdfgsdfgsdfgsdfffffffffffffffffffffffffffffffffff");
			System.out.println("dfgsdfgsdfgsdfgsdfffffffffffffffffffffffffffffffffff");
			for (int i = 0; i < g.length; i++) {
				System.out.println(Bytes.toStringBinary(g[i].getValue())+" kkkkkkkkkkkkkkkkkkkkkkk");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	private static byte[] getColIdbyte(Integer i) {
		
		byte[] ret = Bytes.toBytes(i);
		if (ret.length==4)
			return ret;
		else 
			return null;
	}
}
