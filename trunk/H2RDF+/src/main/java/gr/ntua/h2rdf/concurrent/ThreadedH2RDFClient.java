/*******************************************************************************
 * Copyright [2013] [Nikos Papailiou]
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 ******************************************************************************/
package gr.ntua.h2rdf.concurrent;
import org.apache.zookeeper.KeeperException;

public class ThreadedH2RDFClient extends Thread{
	private Queue q;
	private String query;
	private Producer producer;
	
	public ThreadedH2RDFClient(Queue q, String q2, Producer producer) {
		super("ThreadedClient");
		this.q=q;
		this.query=q2;
		this.producer=producer;
	}
	public void run() {
		 
	    try {
	    	q.produce(query, this.producer);
	 
	    } catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
