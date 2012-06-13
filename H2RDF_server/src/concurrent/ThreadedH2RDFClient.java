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
package concurrent;
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
