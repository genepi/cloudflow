package cloudflow.examples;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.fastq.FastqRecord;
import cloudflow.bio.fastq.SingleRead;
import cloudflow.core.PipelineConf;
import cloudflow.core.operations.Aligner;
import cloudflow.core.operations.CreateFastqPairs;
import cloudflow.core.operations.MapOperation;
import cloudflow.core.records.TextRecord;

public class FastqMapping {


	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Bwa-MEM-Mapper",
				FastqMapping.class);

		pipeline.distributeArchive("jbwa.tar.gz", "jbwa075a.tar.gz");
		pipeline.distributeArchive("reference.tar.gz", "rcrs.tar.gz");
		pipeline.loadFastq(input).apply(CreateFastqPairs.class).groupByKey().apply(Aligner.class).save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
