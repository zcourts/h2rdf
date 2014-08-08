#-------------------------------------------------------------------------------
# Copyright  
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------
Installing H2RDF+v0.2:
Fast deployment:
1. Download http://www.cslab.ece.ntua.gr/~npapa/h2rdfinstall.tar.gz
2. This tar file contains hadoop, hbase(patched version), zookeeper and java all configured and containing the libraries required by H2RDF. The conf files suppose that all these folders are extracted in the /opt directory. If you want to place them somewhere else you need to change the paths in the configuration files.
3. Download and put the patched hbase jar (http://www.cslab.ece.ntua.gr/~npapa/hbase-0.94.5n.jar) in H2RDF's build path.
4. Execute mvn assembly:assembly from the folder of the H2RDF code. This  will generate an targer/H2RDF2.jar
5. Start zookeeper on the master node using bin/zkServer.sh start


Not so fast deployment:
1. Install Hadoop, HBase
2. Add support for hadoop snappy compression (https://code.google.com/p/hadoop-snappy/). Add the .so files in both Hadoop and HBase lib folders.
3. Add the  Hadoop and HBase configuration properties provided in src/main/resources (replace master with the dns name of your master node). Make sure to have hbase-site.xml in Hadoop's conf folder and hdfs-site.xml in Hbase's conf folder.
4. (OPTIONAL) Improve HBase scan performance (http://hadoopstack.com/improving-hbase-scans-round-1/)
5. Export the package gr.ntua.h2rdf.coprocessors into a jar file and copy it to HBase lib folder in all the nodes of the cluster. If everything is set up correctly you should see TranslateIndexEndpoint in the coprocessors list in the WebUI of the regionservers.
6. Execute mvn assembly:assembly from the folder of the H2RDF code. This  will generate an targer/H2RDF2.jar.
7. Copy this jar to one of your cluster nodes(master).
8. Start a zookeeper quorum using port 2181

Load triples:
1. Upload your N-Triples files in HDFS
2. bin/hadoop jar  H2RDF2.jar gr.ntua.h2rdf.loadTriples.LoadTriples <triplesFile> <TableName>

Query example:
1. Start H2RDF server with:
bin/hadoop jar ../H2RDF2.jar gr.ntua.h2rdf.concurrent.SyncPrimitive qTest master dp false
2. Run gr.ntua.h2rdf.examples.QueryExampleOpenRDF. The first time the query will take a lot of time because the server will load the statistics of the table.
3. Watch the server's output to see the progress of the query.
4. The translation of results from the native format of H2RDF+ to RDF is not optimized yet. Please do not take into account the translation time when conducting experiments. 

This version contains some hardcoded values that we plan to put in a configuration file. 
In com.hp.hpl.jena.sparql.algebra.OptimizeOpVisitorDPCaching change the workers line76 to your cluster's mappers.  

