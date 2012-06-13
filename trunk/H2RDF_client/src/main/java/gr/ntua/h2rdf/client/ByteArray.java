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
package gr.ntua.h2rdf.client;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteArray{
	
	private byte[] array;
	
	public ByteArray(byte[] arr)
	{
		array = arr;
	}
	
	public boolean equals(Object o)
	{
		return Bytes.equals(array, ((ByteArray)o).getArray());
	}
	
	public int hashCode()
	{
		return Arrays.hashCode(array);
	}

	public byte[] getArray() {
		return array;
	}

	public void setArray(byte[] array) {
		this.array = array;
	}

}
