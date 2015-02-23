package cloudflow.bio;

public enum ChunkSize {

	MBASES(100000), KBASES(1000);
	
	private int size = 0;
	
	private ChunkSize(int size){
		this.size = size;
	}
	
	public int getSize(){
		return size;
	}
	
}
