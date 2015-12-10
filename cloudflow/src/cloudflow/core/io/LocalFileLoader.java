package cloudflow.core.io;


public interface LocalFileLoader extends ILoader {

	public FileRecordReader createFileRecordReader(String filename);

	public Class<?> getRecordClass();

}
