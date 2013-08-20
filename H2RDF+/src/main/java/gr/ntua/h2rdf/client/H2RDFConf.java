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
package gr.ntua.h2rdf.client;

import java.io.File;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class H2RDFConf {

	private String address;
	private String table;
	private String user;
	private String pool;
	private int algo;
	private Configuration conf;
	
	public Configuration getConf(){
		return conf;
	}
	
	public H2RDFConf(String address, String table, String user) {
		this.address = address;
		this.table = table;
		this.user = user;
		conf = HBaseConfiguration.create();
		try {

            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            
            Document doc = docBuilder.parse (Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("h2rdf-site.xml"));
            
            // normalize text representation
            doc.getDocumentElement().normalize();

            NodeList listOfPersons = doc.getElementsByTagName("property");
            int totalProperties = listOfPersons.getLength();

            for(int s=0; s<totalProperties ; s++){


                Node firstPersonNode = listOfPersons.item(s);
                if(firstPersonNode.getNodeType() == Node.ELEMENT_NODE){


                    Element firstPersonElement = (Element)firstPersonNode;

                    //-------
                    NodeList firstNameList = firstPersonElement.getElementsByTagName("name");
                    Element firstNameElement = (Element)firstNameList.item(0);

                    NodeList textFNList = firstNameElement.getChildNodes();
                    String name = ((Node)textFNList.item(0)).getNodeValue().trim();

                    //-------
                    NodeList lastNameList = firstPersonElement.getElementsByTagName("value");
                    Element lastNameElement = (Element)lastNameList.item(0);

                    NodeList textLNList = lastNameElement.getChildNodes();
                    String value = ((Node)textLNList.item(0)).getNodeValue().trim();
                    if(name.equals("hbase.rootdir")){
                        conf.set(name, "hdfs://"+address+":9000/hbase");
                    }
                    else if(name.equals("hbase.zookeeper.quorum")){
                        conf.set(name, address);
                    }
                    else if(name.equals("fs.default.name")){
                        conf.set(name, "hdfs://"+address+":9000");
                    }
                    else{
                    	conf.set(name, value);
                    }
                }
            }


        }catch (SAXParseException err) {
        	
        	System.out.println ("** Parsing error" + ", line " 
             + err.getLineNumber () + ", uri " + err.getSystemId ());
        	System.out.println(" " + err.getMessage ());

        }catch (SAXException e) {
        	Exception x = e.getException ();
        	((x == null) ? e : x).printStackTrace ();
        	System.exit(1);

        }catch (Throwable t) {
        	t.printStackTrace ();
        }
	}
	
	public void setClusterSize(String clusterSize) {
		if(clusterSize.contains("9")){
			pool="pool9";
		}
		else if(clusterSize.contains("18")){
			pool="pool18";
		}
		else if(clusterSize.contains("27")){
			pool="pool27";
		}
		else if(clusterSize.contains("36")){
			pool="pool36";
		}
		else if(clusterSize.contains("45")){
			pool="pool45";
		}
	}

	public void setJoinAlgorithm(String joinAlgo) {
		if(joinAlgo.contains("MapReduce Partial Input")){
			algo=1;
		}
		else if(joinAlgo.contains("MapReduce Full Input")){
			algo=2;
		}
		else if(joinAlgo.contains("Centralized")){
			algo=3;
		}
		else if(joinAlgo.contains("Adaptive")){
			algo=4;
		}
	}
	
	public String getPool() {
		return pool;
	}

	public void setPool(String pool) {
		this.pool = pool;
	}

	public int getAlgo() {
		return algo;
	}

	public void setAlgo(int algo) {
		this.algo = algo;
	}
	
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getTable() {
		return table;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setTable(String table) {
		this.table = table;
	}

}
