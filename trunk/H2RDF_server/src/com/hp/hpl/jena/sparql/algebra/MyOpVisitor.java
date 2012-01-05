package com.hp.hpl.jena.sparql.algebra;


import java.util.Iterator;
import java.util.List;
import java.util.Set;


import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import partialJoin.JoinPlaner;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.op.*;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprList;


public class MyOpVisitor extends OpVisitorBase{

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
    	

    	Object[] Qo = opBGP.getPattern().getList().toArray();
    	String[] Q = new String[Qo.length];
    	for (int i = 0; i < Qo.length; i++) {
    		Q[i]=Qo[i].toString();
		}
    	
    	Set<Var> vars=query.getQueryPattern().varsMentioned();
    	Object[] v1 = vars.toArray();
    	String[] vs = new String[v1.length];
    	for (int i = 0; i < v1.length; i++) {
			vs[i]=v1[i].toString();
		}
    	JoinPlaner.setid(id);
    	JoinPlaner.newVaRS(vs);
    	JoinPlaner.form(Q);
    	JoinPlaner.removeNonJoiningVaribles(Q);
    	while(!JoinPlaner.isEmpty()){
    		String v = JoinPlaner.getNextJoin();
    		System.out.println(v);
    	}
    }

    public void visit(OpDistinct opDistinct)
    {System.out.println(opDistinct.toString());}

    public void visit(OpSlice opSlice)
    {System.out.println(opSlice.toString());}
}
