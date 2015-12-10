package cloudflow.bio.bam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import cloudflow.core.hadoop.HadoopRecordFileLoader;

public class BamLoader implements HadoopRecordFileLoader {

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
	

	@Override
	public void configure(Configuration conf) {
	
	}

}