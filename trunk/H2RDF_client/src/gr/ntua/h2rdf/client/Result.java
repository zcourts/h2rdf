package gr.ntua.h2rdf.client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;


public class Result {
	
	private HashMap<String,List<String>> results;

	public Result(String line) {
		results = new HashMap<String, List<String>>();
		StringTokenizer tokenizer = new StringTokenizer(line);
		StringTokenizer tok = null;
		while (tokenizer.hasMoreTokens()) {
			String b=tokenizer.nextToken("!");
			if(b.startsWith("?")){
				tok=new StringTokenizer(b);
				String var=tok.nextToken("#");
				var=var.substring(1);
				String bindings = tok.nextToken("#");
				List<String> list =new LinkedList<String>();
				StringTokenizer tok2 = new StringTokenizer(bindings);
				while (tok2.hasMoreTokens()) {
					String bind=tok2.nextToken("_");
					if(!bind.startsWith(" ")){
						list.add(bind);
					}
					
				}
				results.put(var, list);
			}
		}
	}

	@Override
	public String toString() {
		return null;
		//return line;
	}

	public List<String> getBindings(String var) {
		return results.get(var);
	}

	
}
