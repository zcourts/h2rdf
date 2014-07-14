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

import java.io.IOException;
import java.net.ServerSocket;
 
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
