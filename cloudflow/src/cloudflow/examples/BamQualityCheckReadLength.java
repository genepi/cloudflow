package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.bam.BamRecord;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.spark.SparkRunner;

public class BamQualityCheckReadLength {

	static public class CalcReadLength extends
			Transformer<BamRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		public CalcReadLength() {
			super(BamRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(BamRecord record) {

			outRecord.setKey(record.getValue().getBaseQualityString().length()
					+ "");
			outRecord.setValue(1);
			emit(outRecord);

		}

	}

	public static void main(String[] args) throws IOException {

		String mode = args[0];
		String input = args[1];
		String output = args[2];

		BioPipeline pipeline = new BioPipeline("Bam Quality Check Read Length",
				BamQualityCheckReadLength.class);

		pipeline.loadBam(input).apply(CalcReadLength.class).sum().save(output);

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
