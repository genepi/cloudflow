package cloudflow.hadoop.test;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TextLoader implements ILoader{
	@Override
	public Class getInputFormat() {
		return TextInputFormat.class;
	}
}