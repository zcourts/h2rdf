Distributed Indexing and Querying of Big RDF Data using NoSQL and MapReduce

**H2RDF+**

You can find installation details in trunk/H2RDF+v0.2/README.txt

Architectural details of H2RDF+:

http://www.cslab.ece.ntua.gr/~npapa/h2rdfbigdata_paper.pdf

http://www.cslab.ece.ntua.gr/~npapa/sigmod15.pdf

http://www.cslab.ece.ntua.gr/~npapa/h2rdf+sigmod2014.pdf

http://www.cslab.ece.ntua.gr/~npapa/swim2014paper-39.pdf

**H2RDF**

To run H2RDF you first need to install a hadoop and hbase cluster.
H2RDF is compatible with hadoop-1.0.1 and hbase-0-92.0

You also need to install zookeeper-3.3.2

Importing RDF data:
Upload your ntriples file to HDFS and run

bin/hadoop jar H2RDF\_server.jar sampler.SamplerEx input\_Path HBaseTable

Executing SPARQL queries:
Go to your master server and start a zookeeper Quorum by running

bin/zkServer.sh start

In all worker nodes run:

bin/hadoop jar H2RDF\_server.jar concurrent.SyncPrimitive qTest masterDNS c

where masterDNS is the DNS name of your zookeeper master node

You can now run our java query examples from gr.ntua.h2rdf.examples in H2RDF\_client project.


A simpler way to run queries without launching a zookeeper quorum is with the command:

bin/hadoop jar H2RDF\_server.jar partialJoin.Ex2 HBaseTable "SELECT  ?x  WHERE   { ?x rdf:type ub:Publication . ?x ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0> }"

For more details read H2RDF papers:

http://www.cslab.ece.ntua.gr/~npapa/h2rdf_www2012_demo.pdf