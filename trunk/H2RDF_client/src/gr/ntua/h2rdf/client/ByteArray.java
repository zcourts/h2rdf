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
