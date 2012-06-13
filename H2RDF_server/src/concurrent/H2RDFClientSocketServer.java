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
 
public class H2RDFClientSocketServer {
    public static void main(String[] args) throws IOException {
 
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(19898);
        } catch (IOException e) {
            System.err.println("Could not listen on port: 4444.");
            System.exit(1);
        }
        int con=0;
        while(true){
        	System.out.println("new con:"+ con);
        	con++;
	        Socket clientSocket = null;
	        try {
	            clientSocket = serverSocket.accept();
	        } catch (IOException e) {
	            System.err.println("Accept failed.");
	            System.exit(1);
	        }
	 
	        BufferedReader in = new BufferedReader(
	                new InputStreamReader(
	                clientSocket.getInputStream()));
	        String inputLine;
        	//SimplifiedAPI.putTriples("ia200124", "ARCOMEMDB", inputLine);
	        //System.out.println("chunk: "+i);
	        int i=0;
	        while ((inputLine = in.readLine()) != null) { 
		        System.out.println("chunk: "+i);
		        i++;
	        	SimplifiedAPI.putTriples("ia200124", "ARCOMEMDB", inputLine);
		        System.out.println("chunk: "+i);
		        //System.out.println(inputLine); 
	        }

	        
	        in.close();
	        clientSocket.close();
        
        }
        
        //serverSocket.close();
    }
}
