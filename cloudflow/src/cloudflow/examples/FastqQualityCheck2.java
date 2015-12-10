package cloudflow.examples;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.fastq.FastqRecord;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;

public class FastqQualityCheck2 {

	static public class SequenceQual extends
			Transformer<FastqRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		public SequenceQual() {
			super(FastqRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(FastqRecord record) {

			Text qualities = record.getValue().getQuality();
			int sum = 0;
			for (int pos = 0; pos < qualities.getLength(); pos++) {
				sum += qualities.charAt(pos);
			}

			outRecord.setKey(sum / qualities.getLength() + "");
			outRecord.setValue(1);
			emit(outRecord);

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Bam Quality Check",
				FastqQualityCheck2.class);

		pipeline.loadFastq(input).apply(SequenceQual.class).sum().save(output);

		boolean result = new MapReduceRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
