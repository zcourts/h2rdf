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
package gr.ntua.h2rdf.dpplanner;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

public class Split {
	
	public static void main(String[] args) {
		int n=10;
		BitSet S= new BitSet(n);
		S.set(1);
		S.set(3);
		S.set(5);
		S.set(7);
		S.set(9);
		List<BitSet> l = new ArrayList<BitSet>();
		enumerate(l,S);
		
	}

	private static void enumerate(List<BitSet> l, BitSet S) {
		if(S.cardinality()==0){
			System.out.println(l);
			return;
		}
		int min = S.nextSetBit(0);
		S.clear(min);
		PowerSet p = new PowerSet(S);
		for(BitSet b : p){
			b.set(min);
			BitSet St =(BitSet) S.clone();
			St.andNot(b);
			l.add(b);
			enumerate(l, St);
			l.remove(b);
		}
	}
}
