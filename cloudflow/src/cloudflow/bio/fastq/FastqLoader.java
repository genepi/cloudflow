package cloudflow.bio.fastq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import cloudflow.core.io.ILoader;

public class FastqLoader implements ILoader {

	@Override
	public Class getInputFormat() {
		return FastqInputFormat.class;
	}

	@Override
	public Class<?> getInputKeyClass() {
		return Text.class;
	}

	@Override
	public Class<? extends Writable> getInputValueClass() {
		return SequencedFragment.class;
	}

	@Override
	public Class<?> getRecordClass() {
		return FastqRecord.class;
	}


	@Override
	public void configure(Configuration conf) {
	
	}
	
}