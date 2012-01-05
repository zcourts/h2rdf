package partialJoin;

public class Ecount implements Comparable<Ecount> {
    private float eCount;
    private String name;
    
    
    public Ecount(float count, String name) {
		eCount = count;
		this.name = name;
	}


	/**
     * Compare a given Employee with this object.
     * If employee id of this object is 
     * greater than the received object,
     * then this object is greater than the other.
     */
    public int compareTo(Ecount o) {
        return Float.compare(this.eCount ,o.eCount) ;
    }


	public float getECount() {
		return eCount;
	}


	public void setECount(int count) {
		eCount = count;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}
}