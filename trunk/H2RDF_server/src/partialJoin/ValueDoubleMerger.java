package partialJoin;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;

public class ValueDoubleMerger {
	private static Hashtable bindings ;
	private static Enumeration e;
	
	public static void merge(String v, String pat) {
		StringTokenizer t = new StringTokenizer(v);
		String else_val="";
		String[] b = new String[1000];
		int size=0;
		while(t.hasMoreTokens()){
			String binding = t.nextToken("!");
			if(binding.startsWith("?x")){
				b[size]=binding.substring(3);
				size++;
			}
			else{
				else_val+=binding+"!";
			}
		}
		for (int i = 0; i < size; i++) {
			collect(b[i], pat, else_val);
		}
		
	}

	private static void collect(String binding, String pat, String else_val) {
		String tpat, value, el;
		//prepei na kanw binding itterate 
		StringTokenizer bintok = new StringTokenizer(binding);
		while(bintok.hasMoreTokens()){
			String bin = bintok.nextToken("_");
			
			if(bindings.containsKey(bin)){
				value = (String)bindings.get(bin);
				StringTokenizer t = new StringTokenizer(value);
				tpat=t.nextToken("$");
				if(t.hasMoreTokens()){
					el=t.nextToken("$");
				}
				else{
					el="";
				}
				if(!tpat.contains(pat)){
					tpat+=pat;
				}
				el+=else_val;
				bindings.put(bin, tpat+"$"+el);
			}
			else{
				bindings.put(bin, pat+"$"+else_val);
			}
		}
		
		
		
	}

	public static String getValue() {
		String key, tpat, el, value, out="";
		key = e.nextElement().toString();
		value = (String)bindings.get(key);
		StringTokenizer t = new StringTokenizer(value);
		tpat=t.nextToken("$");
		//prepei na elegxo ton arithmo twn pat
		if(tpat.length()>5){
			if(t.hasMoreTokens()){
				el=t.nextToken("$");
			}
			else{
				el="";
			}
			out+="?x#"+key+"_!"+el;
		}
		return out;
	}

	public static void init() {
		bindings= new Hashtable();
	}

	public static boolean hasMore() {
		return e.hasMoreElements();
	}

	public static void itter() {
		e = bindings.keys();
	}

}
