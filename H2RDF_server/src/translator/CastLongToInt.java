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

public class CastLongToInt {
	public static final long LONG_HIGH_BITS = 0x7FFFFFFF00000000L;
	
	public static final long SHORT = 0x7FFF800000000000L;
	public static final long BYTE = 0x7F80000000000000L;

	public static int castLong(long l) {
		long high = l & LONG_HIGH_BITS;
		high = high >> 32+14;

		return Integer.parseInt(high+"");
	}
	
	public static int castLongToShort(long l) {
		long high = l & SHORT;
		high = high >> 47;
		int ret = (int) high;

		return ret;
	}
	
	public static int compressLong(long l) {
		long high = l & BYTE;
		high = high >> (47+8);
		int ret = (short) high;
		//ret = ret;
		return ret;
	}
	
    public static void main(String[] args)
    {
    	long l= Long.MIN_VALUE;
    	int il= compressLong(l);
    	System.out.println(il+" "+Integer.MIN_VALUE);
    	l= Long.MAX_VALUE;
    	il= compressLong(l);
    	System.out.println(il+" "+Integer.MAX_VALUE);
    	l= new Long("-1334454566668888885");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("-334454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("-34454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("-4454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("-454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("-54566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("0");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("1334454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("334454566665");
    	il= castLong(l);
    	System.out.println(il);
    	l= new Long("34454566665");
    	il= castLong(l);
    	System.out.println(il);
    	l= new Long("4454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("454566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("54566665");
    	il= compressLong(l);
    	System.out.println(il);
    	l= new Long("0");
    	il= compressLong(l);
    	System.out.println(il);
    }
}
