package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.bam.BamRecord;
import cloudflow.core.Pipeline;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.spark.SparkRunner;

public class BamQualityCheckSpark {

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
				outRecord.setValue((int)record.getValue().getBaseQualities()[pos]);
				emit(outRecord);
			}

		}

	}

	public static void main(String[] args) throws IOException {

		String input = "../test-data/test.bam";
		String output = "output-bam";
;

		BioPipeline pipeline = new BioPipeline("Bam Quality Check running on Spark",
				BamQualityCheckSpark.class);

		pipeline.loadBam(input).apply(SplitByPos.class).mean().save(output);

		boolean result = new SparkRunner("local").run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
