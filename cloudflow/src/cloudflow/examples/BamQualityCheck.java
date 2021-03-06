package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.bam.BamRecord;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.spark.SparkRunner;

public class BamQualityCheck {

	static public class SplitByPos extends
			Transformer<BamRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		public SplitByPos() {
			super(BamRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(BamRecord record) {

			for (int pos = 0; pos < record.getValue().getReadLength(); pos++) {
				outRecord.setKey(pos + "");
				outRecord
						.setValue((int) record.getValue().getBaseQualities()[pos]);
				emit(outRecord);
			}

		}

	}

	public static void main(String[] args) throws IOException {

		String mode = args[0];
		String input = args[1];
		String output = args[2];

		BioPipeline pipeline = new BioPipeline("Bam Quality Check",
				BamQualityCheck.class);

		pipeline.loadBam(input).apply(SplitByPos.class).mean().save(output);

		if (mode.equals("mapreduce")) {
			System.out.println("Running pipeline using mapreduce");
			boolean result = new MapReduceRunner().run(pipeline);
			if (!result) {
				System.exit(1);
			}
			return;
		}
		
		if (mode.equals("spark")) {
			System.out.println("Running pipeline using spark");
			boolean result = new SparkRunner("yarn").run(pipeline);
			if (!result) {
				System.exit(1);
			}
			return;
		}
		
		System.out.println("unknown mode.");
		
	}
}
