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

public class SizePowerSet {
    private ArrayList<Integer> arr = null;
    private BitSet bset = null;
    private BitSet ret = null;
    private BitSet lowlevelBSet = null;
	private Iterator<Integer> oneLevelit;
	private int size, bsetsize;
	private SizePowerSet lowerSet;
	private int first, previousOneLevel;

    @SuppressWarnings("unchecked")
    public SizePowerSet(BitSet set, int size)
    {
    	this.size=size;
    	bset=set;
    	bsetsize = set.size();
    	arr = new ArrayList<Integer>();
        for (int i = 0; i <= set.size(); i++) {
        	if(set.get(i))
        		arr.add(i);
		}
        oneLevelit = arr.iterator();
    	if(size>1){
    		lowerSet = new SizePowerSet(set,size-1);
    	}
    	else if(size==0){
    		first=0;
    	}
    	previousOneLevel=0;
    }

    public BitSet next() {
    	if(size==1){
    		if(oneLevelit.hasNext()){
    	    	BitSet ret = new BitSet(bsetsize);
    			ret.set(oneLevelit.next());
    			return ret;
    		}
    		else{
    			return null;
    		}
    	}
    	else if(size>1){
    		if(oneLevelit.hasNext()){
    			if(lowlevelBSet==null){
    				lowlevelBSet= lowerSet.next();
    				if(lowlevelBSet==null)
    					return null;
    			}
    	    	//BitSet ret = (BitSet)lowlevelBSet.clone();
    			int t = oneLevelit.next();
				lowlevelBSet.clear(previousOneLevel);
    			int minLowlevel = lowlevelBSet.nextSetBit(0);
    			if(t<minLowlevel){
    				lowlevelBSet.set(t);
    				previousOneLevel=t;
    				return lowlevelBSet;
    			}
    			else{
        			//restart oneLevelBitSet
        	        oneLevelit = arr.iterator();
        	        //next lowlevel
        			lowlevelBSet = lowerSet.next();
        			previousOneLevel=0;
        			if(lowlevelBSet==null)
        				return null;
    				return next();
    			}
    		}

			lowlevelBSet = lowerSet.next();
			previousOneLevel=0;
			if(lowlevelBSet==null)
				return null;
    		else {
    			//restart oneLevelBitSet
    	        oneLevelit = arr.iterator();
    	        //next lowlevel
    			int t = oneLevelit.next();
    			int minLowlevel = lowlevelBSet.nextSetBit(0);
    			if(t<minLowlevel){
    				lowlevelBSet.set(t);
    				previousOneLevel=t;
    				return lowlevelBSet;
    			}
    			else{
        			//restart oneLevelBitSet
        	        oneLevelit = arr.iterator();
        	        //next lowlevel
        			lowlevelBSet = lowerSet.next();
        			previousOneLevel=0;
        			if(lowlevelBSet==null)
        				return null;
    				return next();
    			}
    		}
    	}
    	else if(size==0 && first==0){
    		first++;
    		return new BitSet(bsetsize);
    	}
    	return null;
    }

    public static void main(String[] args) {
    	long time =System.currentTimeMillis();
    	int n =10;
		BitSet b = new BitSet(n);
		for (int i = 1; i <= n; i++) {
			b.set(i);
		}
		int count =0;
		for(int i = 0; i <= n; i++){
			SizePowerSet p = new SizePowerSet(b, i);
			BitSet b1;
			while((b1 = p.next())!=null){
				//System.out.println(b1);
				count++;
			}
		}
		System.out.println("Count: "+count);
    	long stoptime =System.currentTimeMillis();
    	System.out.println("Time ms: "+(stoptime-time));
	}
}
