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

import org.apache.hadoop.hbase.util.Bytes;

import gr.ntua.h2rdf.client.H2RDFConf;
import gr.ntua.h2rdf.client.JavaApiCall;

public class RpcZookeeper {
    private JavaApiCall call;

    public RpcZookeeper(String host) {
        String table = "ARCOMEMDB";
        String user = "arcomem";
        H2RDFConf conf = new H2RDFConf(host, table, user);
        call = new JavaApiCall(conf);
    }

    public void close() {
        if (call != null)
            call.close();
        call = null;
    }

    public void finalize() {
        close();
    }

    public byte[] rpc(String func, byte[] args) throws Exception {
        byte[] type = {2};
        byte[] buf =
            cat(type,
                cat(("[" + func + ", ").getBytes("UTF-8"),
                    cat(args, "]".getBytes("UTF-8"))));
        try {
            return call.send(buf);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public byte[] cat(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}
