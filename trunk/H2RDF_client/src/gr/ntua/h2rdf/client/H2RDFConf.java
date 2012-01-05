package gr.ntua.h2rdf.client;

public class H2RDFConf {

	private String address;
	private String name;
	
	public H2RDFConf(String address, String name) {
		this.address = address;
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
