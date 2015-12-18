package cloudflow.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.fastq.FastqRecord;
import cloudflow.core.local.LocalRunner;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;

public class FastqQualityCheckLocal {

	static public class SplitByPos extends Transformer<FastqRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		public SplitByPos() {
			super(FastqRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(FastqRecord record) {

			Text qualities = record.getValue().getQuality();

			for (int pos = 0; pos < qualities.getLength(); pos++) {
				outRecord.setKey(pos + "");
				outRecord.setValue(qualities.charAt(pos));
				emit(outRecord);
			}

		}

	}

	public static void main(String[] args) throws IOException {

		String input = "../test-data/test.bam";
		String output = "output.txt";

		BioPipeline pipeline = new BioPipeline("Bam Quality Check",
				FastqQualityCheckLocal.class);

		pipeline.loadFastq(input).apply(SplitByPos.class).mean().save(output);

		boolean result = new LocalRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
