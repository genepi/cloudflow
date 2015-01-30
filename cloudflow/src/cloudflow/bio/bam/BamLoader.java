package cloudflow.bio.bam;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import cloudflow.core.io.ILoader;

public class BamLoader implements ILoader {

	@Override
	public Class getInputFormat() {
		return BAMInputFormat.class;
	}

	@Override
	public Class<?> getInputKeyClass() {
		return LongWritable.class;
	}

	@Override
	public Class<? extends Writable> getInputValueClass() {
		return SAMRecordWritable.class;
	}

	@Override
	public Class<?> getRecordClass() {
		return BamRecord.class;
	}

}