package cloudflow.hadoop.old;

public class InputFile {

	private InputFile(){
		
	}
	
	private InputFile(String content){
		
	}
	
	public static InputFile fillWith(String content){
		return new InputFile(content);
	}
	
	public String getFilename(){
		return null;
	}
	
}
