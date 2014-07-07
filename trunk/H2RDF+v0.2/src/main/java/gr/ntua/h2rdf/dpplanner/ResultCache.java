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
import java.util.HashMap;
import java.util.List;

public class ResultCache {
	
	public ResultCache() {
		// TODO Auto-generated constructor stub
	}
	
	public void initialize() {
		
	}
	
	public static void main(String[] args) {
		ResultCache cache = new ResultCache();
		
		HashMap<String,List<String>> query = new HashMap<String, List<String>>();
		
		List<String> l = new ArrayList<String>(); 
		l.add("2 ?y");
		l.add("4 ?n");
		l.add("7 ?l");
		query.put("?x", l);
		
		l = new ArrayList<String>(); 
		l.add("2 ?x");
		query.put("?y", l);
		
		l = new ArrayList<String>(); 
		l.add("4 ?x");
		query.put("?n", l);
		
		l = new ArrayList<String>(); 
		l.add("7 ?x");
		query.put("?l", l);
		
		
	}
}
