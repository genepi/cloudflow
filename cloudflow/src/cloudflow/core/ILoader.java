package cloudflow.core;

import org.apache.hadoop.mapreduce.InputFormat;

public interface ILoader {

	public Class<InputFormat<?, ?>> getInputFormat();

	public Class<?> getInputKeyClass();

	public Class<?> getInputValueClass();
	
	public Class<?> getRecordClass();
	

}
