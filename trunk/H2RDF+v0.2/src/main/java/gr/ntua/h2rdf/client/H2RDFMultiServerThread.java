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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
 
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
        //System.out.println("chunk: "+i);
        i++;
    	//SimplifiedAPI.putTriples("ia200124", "ARCOMEMDB", inputLine);
        //System.out.println("chunk: "+i);
        while ((inputLine = in.readLine()) != null) { 
	        //System.out.println("chunk: "+i);
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
