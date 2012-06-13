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
package gr.ntua.h2rdf.bytes;

public class NotSupportedDatatypeException extends Exception {

	public NotSupportedDatatypeException(String type) {
		super(type);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -7954121200863455224L;

}
