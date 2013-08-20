/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.coprocessors;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TreePosition implements Comparable<TreePosition>{

	public long[] position;

	public TreePosition(long[] position) {
		this.position = position;
	}

	@Override
	public int compareTo(TreePosition o) {
		long[] temp = o.position;
		int minLength = temp.length;
		int eq = 1;
		if(minLength>position.length){
			minLength = position.length;
			eq = -1;
		}
		else if(minLength == position.length)
			eq=0;
		
		for (int i = 0; i < minLength; i++) {
			if(position[i] > temp[i])
				return 1;
			else if(position[i] < temp[i])
				return -1;
		}
		
		return eq;
	}
	
	@Override
	protected TreePosition clone() {
		return new TreePosition(position.clone());
	}
	
	public void increaseLevel() {
		long[] temp = position;
		position = new long[temp.length+1];
		System.arraycopy(temp, 0, position, 0, temp.length);
	}
	

	public void goToLevel(int level) {
		long[] temp = position.clone();
		position = new long[level + 1];
		System.arraycopy(temp, 0, position, 0, level+1);
	}
	
	/*
	 * pos should be greater than position
	 */
	public List<Long> computeJump(TreePosition pos) {
		int maxPos = position.length-1;
		for (int i = 0; i < position.length; i++) {
			if(position[i] != pos.position[i]){
				maxPos = i;
				break;
			}
		}
		List<Long> ret = new LinkedList<Long>();
		for (int i = position.length-1; i > maxPos; i--) {
			ret.add(new Long(pos.position[i]));
		}
		ret.add(new Long(pos.position[maxPos]-position[maxPos]));
		return ret;
	}

	public void increaseLastLevelValue(int size) {
		position[position.length-1]+=size;
	}

	public void increaseSecondLastLevelValue(int size) {
		position[position.length-2]+=size;
		
	}

	public boolean subPosition(TreePosition pos, int level) {
		if(this.position.length > pos.position.length)
			return false;
		if(this.position.length < level+1)
			return false;
		
		for (int j = 0; j < level+1; j++) {
			if(position[j]!=pos.position[j])
				return false;
		}
		return true;
		
	}

	public void print() {
		for (int i = 0; i < position.length; i++) {
			System.out.print(position[i]+".");
		}
		System.out.print(" ");
	}


	public static void main(String[] args) {
		long[] l1 = {1, 3, 4, 2};
		TreePosition p1 = new TreePosition(l1);
		long[] l2 = {24, 7 ,0, 3};
		TreePosition p2 = new TreePosition(l2);
		
		Iterator<Long> r = p1.computeJump(p2).iterator();
		while(r.hasNext()){
			System.out.println(r.next());
		}
		
		//System.out.println(p1.compareTo(p2));
	}


}
