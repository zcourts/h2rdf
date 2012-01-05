package partialJoin;

public class ValueMerger {
	private static String value = "";
	
	public static void merge(String v, String pat) {
		value+=v;
	}

	public static String getValue() {
		return value;
	}

	public static void init() {
		value = "";
	}

}
