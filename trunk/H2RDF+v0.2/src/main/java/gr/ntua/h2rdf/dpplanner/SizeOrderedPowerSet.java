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
import java.util.BitSet;

public class SizeOrderedPowerSet {
	private int level;
	private SizePowerSet pset;
	private BitSet set;
	
    @SuppressWarnings("unchecked")
    public SizeOrderedPowerSet(BitSet set)
    {
    	this.set =set;
    	level=0;
    	pset = new SizePowerSet(set, level);
    }

    public BitSet next() {
    	BitSet ret = pset.next();
    	if(ret==null ){
    		if( level==set.cardinality()){
    			return null;
    		}
    		else{
            	level++;
            	pset = new SizePowerSet(set, level);
            	ret = pset.next();
            	return ret;
    		}
    	}
    	else{
        	return ret;
    	}
    }

    public static void main(String[] args) {
    	long time =System.currentTimeMillis();
    	int n =10;
		BitSet b = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			b.set(i);
		}
		int count =0;
		SizeOrderedPowerSet p = new SizeOrderedPowerSet(b);
		BitSet b1;
		while((b1 = p.next())!=null){
			System.out.println(b1);
			count++;
		}
		System.out.println("Count: "+count);
    	long stoptime =System.currentTimeMillis();
    	System.out.println("Time ms: "+(stoptime-time));
	}
}
