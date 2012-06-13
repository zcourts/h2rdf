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
 
public class H2RDFMultiClientSocketServer {
    public static void main(String[] args) throws IOException {
 
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(19898);
        } catch (IOException e) {
            System.err.println("Could not listen on port: 4444.");
            System.exit(1);
        }
        int con=0;
        while (true){
        	System.out.println("Thread: "+con);
            new H2RDFMultiServerThread(serverSocket.accept()).start();
            con++;
        }
        
        //serverSocket.close();
    }
}
