package cloudflow.core.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;

import cloudflow.core.io.ILoader;
import cloudflow.core.records.Record;

public interface HadoopRecordFileLoader<r extends Record> extends ILoader {

	public Class<InputFormat<?, ?>> getInputFormat();

	public Class<?> getInputKeyClass();

	public Class<?> getInputValueClass();

	public void configure(Configuration conf);

}
