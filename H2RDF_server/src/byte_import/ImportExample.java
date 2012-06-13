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
package byte_import;

/** 
*/

public class ImportExample
{
	 public static void main(String[] args)
	 {
	     // first argument the file of dataset
		     MyModelFactory.createBulkModel(args) ;
	 }
}
