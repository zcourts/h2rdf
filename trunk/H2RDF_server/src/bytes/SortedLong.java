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
package bytes;

public class SortedLong {
	public static final long FIRST_BIT = 0x7FFFFFFFFFFFFFFFL;
	public static final long POSSITIVE = 0x8000000000000000L;
	public static final long NEGATIVE = 0x0000000000000000L;

	public static long toSortedLong(long l){
		long ret= l & FIRST_BIT;
		if(l<0){
			ret=ret | NEGATIVE;
		}
		else{
			ret=ret | POSSITIVE;
		}
		return ret;
	}
	
	public static long toLong(long l){
		long ret= l & FIRST_BIT;
		if(l<0){
			ret=ret | NEGATIVE;
		}
		else{
			ret=ret | POSSITIVE;
		}
		return ret;
	}
	
	
	public static void main(String[] args)
    {
    	long l= Long.MIN_VALUE;
    	long il= toSortedLong(l);
    	long il2= toLong(il);
    	System.out.println(l+" \t"+Long.toBinaryString(il)+"\t\t\t\t\t\t\t\t\t"+il2);
    	l= new Long("-7334454566668888885");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-5334454566668888885");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-1334454566668888885");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-334454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-34454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-4454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-54566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("-1");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t\t"+Long.toBinaryString(il)+"\t\t"+il2);
    	l= new Long("0");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= new Long("54566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= new Long("454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= new Long("4454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= new Long("34454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= new Long("334454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= new Long("1334454566665");
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t\t"+Long.toBinaryString(il)+"\t"+il2);
    	l= Long.MAX_VALUE;
    	il= toSortedLong(l);
    	il2= toLong(il);
    	System.out.println(l+" \t"+Long.toBinaryString(il)+"\t"+il2);
    }
}
