/*******************************************************************************
 * Copyright 2014 Nikolaos Papailiou
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class BulkLoader implements Loader {

	private int localChunkSize;
	private ApiExecutor apiExecutor;
	//private String triples;
	private ByteArrayOutputStream out;
	private int count;
	private H2RDFConf conf;

	public BulkLoader(H2RDFConf conf, ApiExecutor apiExecutor) {
		this.apiExecutor = apiExecutor;
		this.conf = conf;
		out = new ByteArrayOutputStream();
		//triples = "";
		count =0;
	}

	@Override
	public void add(Triple triple) {
		Model model=ModelFactory.createDefaultModel();
		model.add(model.asStatement(triple));
		model.write(out, "N-TRIPLE");
		
		//triples+= triple.getSubject()+" "+triple.getPredicate()+" "+triple.getObject()+" . \n";
		count++;
		if(count>=localChunkSize){
			try {
				out.flush();
				apiExecutor.bulkPutTriples(conf.getTable(), out.toString("UTF-8"));
				out.reset();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//triples ="";
			count=0;
		}
	}
	
	public void setLocalChunkSize(int localChunkSize) {
		this.localChunkSize=localChunkSize;
	}
	
	@Override
	public void close() {
		if(count>0){
			try {
				out.flush();
				apiExecutor.bulkPutTriples(conf.getTable(), out.toString("UTF-8"));
				out.reset();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public void bulkLoadTriples() {
		if(count>0){
			try {
				out.flush();
				apiExecutor.bulkPutTriples(conf.getTable(), out.toString("UTF-8"));
				out.reset();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		apiExecutor.bulkLoadTriples(conf.getTable());
	}

	@Override
	public void delete(Triple triple) {
		// TODO Auto-generated method stub
		
	}

}
