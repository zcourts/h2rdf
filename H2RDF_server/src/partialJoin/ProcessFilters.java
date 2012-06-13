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

import org.apache.hadoop.hbase.util.Bytes;

import bytes.ByteValues;
import bytes.NotSupportedDatatypeException;

import com.hp.hpl.jena.sparql.expr.ExprFunction;

public class ProcessFilters {

	public static void process(byte[] startfilter, byte[] stopfilter,
			ExprFunction filter) throws Exception {
		//System.out.println(filter.toString());
		//System.out.println(filter.getArg(1) +" " +filter.getArg(2));
		String op = filter.getOpName();
		byte[] temp = null;
		if(op.equals("<")){
			if(filter.getArg(1).isVariable()){
				temp=ByteValues.getFullValue(filter.getArg(2).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(2).getConstant().asNode().getLiteralDatatypeURI());
				processLower(startfilter, stopfilter, temp);
			}
			else{
				temp=ByteValues.getFullValue(filter.getArg(1).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(1).getConstant().asNode().getLiteralDatatypeURI());
				processGreater(startfilter, stopfilter, temp);
			}
		}
		else if(op.equals("<=")){
			if(filter.getArg(1).isVariable()){
				temp=ByteValues.getFullValue(filter.getArg(2).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(2).getConstant().asNode().getLiteralDatatypeURI());
				processLowerEqual(startfilter, stopfilter, temp);
			}
			else{
				temp=ByteValues.getFullValue(filter.getArg(1).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(1).getConstant().asNode().getLiteralDatatypeURI());
				processGreaterEqual(startfilter, stopfilter, temp);
			}
		}
		else if(op.equals(">")){
			if(filter.getArg(1).isVariable()){
				temp=ByteValues.getFullValue(filter.getArg(2).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(2).getConstant().asNode().getLiteralDatatypeURI());
				processGreater(startfilter, stopfilter, temp);
			}
			else{
				temp=ByteValues.getFullValue(filter.getArg(1).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(1).getConstant().asNode().getLiteralDatatypeURI());
				processLower(startfilter, stopfilter, temp);
			}
		}
		else if(op.equals(">=")){
			if(filter.getArg(1).isVariable()){
				temp=ByteValues.getFullValue(filter.getArg(2).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(2).getConstant().asNode().getLiteralDatatypeURI());
				processGreaterEqual(startfilter, stopfilter, temp);
			}
			else{
				temp=ByteValues.getFullValue(filter.getArg(1).getConstant().asNode().getLiteralLexicalForm(),
						filter.getArg(1).getConstant().asNode().getLiteralDatatypeURI());
				processLowerEqual(startfilter, stopfilter, temp);
			}
		}
		else{
			throw new Exception("usupported filter type"+filter.toString());
		}
		
	}

	private static void processGreaterEqual(byte[] startfilter,
			byte[] stopfilter, byte[] temp) {
		if(Bytes.compareTo(startfilter, temp)<0){
			for (int i = 0; i < temp.length; i++) {
				startfilter[i]=temp[i];
			}
		}
	}

	private static void processLowerEqual(byte[] startfilter,
			byte[] stopfilter, byte[] temp) {
		if(Bytes.compareTo(stopfilter, temp)>0){
			for (int i = 0; i < temp.length; i++) {
				stopfilter[i]=temp[i];
			}
		}
	}

	private static void processGreater(byte[] startfilter, byte[] stopfilter,
			byte[] temp) {
		temp=Bytes.incrementBytes(temp, new Long(1));
		if(Bytes.compareTo(startfilter, temp)<0){
			for (int i = 0; i < temp.length; i++) {
				startfilter[i]=temp[i];
			}
		}
	}

	private static void processLower(byte[] startfilter, byte[] stopfilter,
			byte[] temp) {
		temp=Bytes.incrementBytes(temp, new Long(-1));
		if(Bytes.compareTo(stopfilter, temp)>0){
			for (int i = 0; i < temp.length; i++) {
				stopfilter[i]=temp[i];
			}
		}
	}
}
