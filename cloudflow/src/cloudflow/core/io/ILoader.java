package cloudflow.core.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

public interface ILoader {

	public Class<InputFormat<?, ?>> getInputFormat();

	public Class<?> getInputKeyClass();

	public Class<?> getInputValueClass();
	
	public Class<?> getRecordClass();
	
	public void configure(Configuration conf);
	

}
