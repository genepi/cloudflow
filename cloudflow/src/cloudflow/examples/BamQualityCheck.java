package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.bam.BamRecord;
import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.operations.MapStep;
import cloudflow.core.operations.ReduceStep;
import cloudflow.core.records.IntFloatRecord;
import cloudflow.core.records.IntIntRecord;

public class BamQualityCheck {

	static public class SplitByPos extends MapStep<BamRecord, IntIntRecord> {

		IntIntRecord outRecord = new IntIntRecord();

		@Override
		public void process(BamRecord record) {

			for (int pos = 0; pos < record.getValue().getReadLength(); pos++) {
				outRecord.setKey(pos);
				outRecord.setValue(record.getValue().getBaseQualities()[pos]);
				createRecord(outRecord);
			}

		}

	}

	static public class Mean extends ReduceStep<IntIntRecord, IntFloatRecord> {

		private IntFloatRecord outRecord = new IntFloatRecord();

		// TODO: atm: key is converted to String. --> change!!

		@Override
		public void process(String key, RecordValues<IntIntRecord> values) {

			int sum = 0;
			int count = 0;
			while (values.hasNextRecord()) {
				sum += values.getRecord().getValue();
				count++;
			}
			outRecord.setKey(Integer.parseInt(key));
			outRecord.setValue(sum / (float) count);
			createRecord(outRecord);
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Bam Quality Check",
				BamQualityCheck.class);

		pipeline.loadBam(input).perform(SplitByPos.class, IntIntRecord.class)
				.groupByKey().perform(Mean.class).save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
