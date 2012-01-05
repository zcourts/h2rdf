package translator;

public class SingletonHashset {

	static int isInitialized=0;

	public SingletonHashset() {
		if(isInitialized==0){
			isInitialized=1;
			
		}
	}
	
}
