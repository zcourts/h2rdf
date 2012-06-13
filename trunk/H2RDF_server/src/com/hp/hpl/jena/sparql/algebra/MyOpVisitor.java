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
package com.hp.hpl.jena.sparql.algebra;


import java.util.Iterator;
import java.util.List;
import java.util.Set;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import partialJoin.JoinPlaner;
import partialJoin.QueryProcessor;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.op.*;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.PatternVars;


public class MyOpVisitor extends OpVisitorBase {

	private Query query;
	private String id;
	private OpBGP opBGP;
	
	public MyOpVisitor(String id, Query query) {
		this.query = query;
		this.id = id;
	}
	
    public void visit(OpBGP opBGP)
    {
    	this.opBGP = opBGP;
    }

    public void visit(OpJoin opJoin)
    {System.out.println(opJoin.toString());}

    public void visit(OpLeftJoin opLeftJoin)
    {System.out.println(opLeftJoin.toString());}

    public void visit(OpUnion opUnion)
    {System.out.println(opUnion.toString());}

    public void visit(OpFilter opFilter)
    {
    	//JoinPlanner.setFilterVars();
    	Iterator<Expr> it = opFilter.getExprs().iterator();
    	while(it.hasNext()){
    		Expr e =it.next();
    		Iterator<Expr> a = e.getFunction().getArgs().iterator();
			System.out.println(e.getFunction().getOpName());
    		while(a.hasNext()){
    			Expr temp = a.next();
    			if(temp.isVariable())
    				JoinPlaner.filter(temp.toString(),e.getFunction());
    		}
    	}
    }

    public void visit(OpGraph opGraph)
    {System.out.println(opGraph.toString());}

    public void visit(OpQuadPattern quadPattern)
    {System.out.println(quadPattern.toString());}

    public void visit(OpDatasetNames dsNames)
    {System.out.println(dsNames.toString());}

    public void visit(OpTable table)
    {System.out.println(table.toString());}

    public void visit(OpExt opExt)
    {System.out.println(opExt.toString());}

    public void visit(OpOrder opOrder)
    {System.out.println(opOrder.toString());}

    public void visit(OpProject opProject)
    {
		Configuration conf = new Configuration();
    	FileSystem fs = null;
    	try {
    		fs = FileSystem.get(conf);
    		Path out =new Path("output");
    		if(!fs.exists(out)){
    			fs.delete(out,true);
    			fs.mkdirs(out);
    		}
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	//Iterator<Triple> it = opBGP.getPattern().getList().iterator();
    	/*String[] Q1 = new String[opBGP.getPattern().getList().size()];
    	int i =0;
    	while(it.hasNext()){
    		Triple t = it.next();
    		Q1[i]=t.getSubject().toString(false)+"$^^$"+t.getPredicate().toString(false)+"$^^$"+t.getObject().toString(false);
        	System.out.print(t.getSubject().toString(false));
        	System.out.print(t.getPredicate().toString(false));
        	System.out.print(t.getObject().toString(false));
        	System.out.println();
        	i++;
    	}*/
    	
    	
    	/*Object[] Qo = opBGP.getPattern().getList().toArray();
    	String[] Q = new String[Qo.length];
    	for (int i = 0; i < Qo.length; i++) {
        	System.out.println(Qo[i].toString());
    		Q[i]=Qo[i].toString();
		}*/
    	
    	/*Iterator<Var> it2 = vars.iterator();
    	String[] vs = new String[vars.size()];
    	i =0;
    	while(it2.hasNext()){
    		Var v = it2.next();			
    		vs[i]=v.toString();
    		i++;
    		//System.out.println(v.toString());
    	}*/
    	
    	//Set<Var> vars=query.getQueryPattern().varsMentioned(); deprecated
    	/*Object[] v1 = vars.toArray();
    	String[] vs = new String[v1.length];
    	for (int i = 0; i < v1.length; i++) {
        	System.out.println(v1[i].toString());
			vs[i]=v1[i].toString();
		}*/
    	
    	Triple[] Q= new Triple[0];
    	Q = opBGP.getPattern().getList().toArray(Q);
    	Set<Var> vars = PatternVars.vars(query.getQueryPattern());
    	
    	JoinPlaner.setid(id);
    	JoinPlaner.newVaRS(vars);
    	try {
			JoinPlaner.form(Q);
	    	JoinPlaner.removeNonJoiningVaribles(Q);
	    	int i=0;
	    	while(!JoinPlaner.isEmpty()){
	    		String v = JoinPlaner.getNextJoin();
	    		System.out.println(v);
	    		i++;
	    	}
	    	if(i==0 ){
	    		Path outFile=new Path("output/Join_"+id+"_"+0);
				if (fs.exists(outFile)) {
					fs.delete(outFile,true);
				}
				QueryProcessor.executeSelect(Q[0], fs.create(outFile), "P0");
	    	}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    }

    public void visit(OpDistinct opDistinct)
    {System.out.println(opDistinct.toString());}

    public void visit(OpSlice opSlice)
    {System.out.println(opSlice.toString());}
}
