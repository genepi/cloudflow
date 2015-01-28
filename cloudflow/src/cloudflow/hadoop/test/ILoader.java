package cloudflow.hadoop.test;

import org.apache.hadoop.mapreduce.InputFormat;


public interface ILoader {

	public Class<InputFormat<?, ?>> getInputFormat();
	
}
