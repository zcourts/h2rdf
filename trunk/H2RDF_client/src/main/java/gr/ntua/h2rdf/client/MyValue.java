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
package gr.ntua.h2rdf.client;

import org.openrdf.model.Value;

public class MyValue implements Value {
	private String val;
	public MyValue(String val) {
		this.val=val;
	}

	@Override
	public String stringValue() {
		return val;
	}

	@Override
	public String toString() {
		return val;
	}

}
