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
