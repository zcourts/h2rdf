Isntalling H2RDF+:

1. Install Hadoop, HBase
2. Add support for hadoop snappy compression (https://code.google.com/p/hadoop-snappy/). Add the .so files in both Hadoop and HBase lib folders.
3. Add the  Hadoop and HBase configuration properties provided in src/main/resources (replace master with the dns name of your master node). Make sure to have hbase-site.xml in Hadoop's conf folder and hdfs-site.xml in Hbase's conf folder.
4. (OPTIONAL) Improve HBase scan performance (http://hadoopstack.com/improving-hbase-scans-round-1/)
5. Export the package gr.ntua.h2rdf.coprocessors into a jar file and copy it to HBase lib folder in all the nodes of the cluster. If everything is set up correctly you should see TranslateIndexEndpoint in the coprocessors list in the WebUI of the regionservers.
6. Upload your N-Triples files in HDFS
7. Execute mvn assembly:assembly from the folder of the H2RDF code. This  will generate an targer/H2RDF.jar.
8. Copy this jar to one of your cluster nodes(master).
9. Load triples using:
bin/hadoop jar  H2RDF.jar gr.ntua.h2rdf.loadTriples.LoadTriples <triplesFile> <TableName>
10. Query example:
bin/hadoop jar H2RDF.jar gr.ntua.h2rdf.queryProcessing.QueryExample
