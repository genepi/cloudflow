package cloudflow.bio.vcf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import cloudflow.core.io.ILoader;

public class VcfLoader implements ILoader {

	@Override
	public Class getInputFormat() {
		return VCFInputFormat.class;
	}

	@Override
	public Class<?> getInputKeyClass() {
		return LongWritable.class;
	}

	@Override
	public Class<? extends Writable> getInputValueClass() {
		return VariantContextWritable.class;
	}

	@Override
	public Class<?> getRecordClass() {
		return VcfRecord.class;
	}

	@Override
	public void configure(Configuration conf) {

	}

}