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

import gr.ntua.h2rdf.client.SimplifiedAPI;

import java.net.*;
import java.io.*;
 
public class H2RDFMultiServerThread extends Thread {
    private Socket socket = null;
 
    public H2RDFMultiServerThread(Socket socket) {
	    super("H2RDFMultiServerThread");
	    this.socket = socket;
    }
 
    public void run() {
 
    try {
 
        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                		socket.getInputStream()));
        String inputLine;
        int i=0;
        inputLine = in.readLine();
        System.out.println("chunk: "+i);
        i++;
    	//SimplifiedAPI.putTriples("ia200124", "ARCOMEMDB", inputLine);
        //System.out.println("chunk: "+i);
        while ((inputLine = in.readLine()) != null) { 
	        System.out.println("chunk: "+i);
	        i++;
        	SimplifiedAPI.putTriples("ia200124", "ARCOMEMDB", inputLine);
	        System.out.println("chunk: "+i);
	        //System.out.println(inputLine); 
        }

        
        in.close();
        socket.close();
 
    } catch (IOException e) {
        e.printStackTrace();
    }
    }
}
