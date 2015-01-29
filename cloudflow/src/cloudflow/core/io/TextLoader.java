package cloudflow.core.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import cloudflow.core.records.TextRecord;
import cloudflow.core.ILoader;

public class TextLoader implements ILoader {

	@Override
	public Class getInputFormat() {
		return TextInputFormat.class;
	}

	@Override
	public Class<?> getInputKeyClass() {
		return Object.class;
	}

	@Override
	public Class<? extends Writable> getInputValueClass() {
		return Text.class;
	}

	@Override
	public Class<?> getRecordClass() {
		return TextRecord.class;
	}

}