package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.bam.BamRecord;
import cloudflow.core.operations.MapStep;
import cloudflow.core.records.IntIntRecord;

public class BamQualityCheck {

	static public class SplitByPos extends MapStep<BamRecord, IntIntRecord> {

		IntIntRecord outRecord = new IntIntRecord();

		public SplitByPos() {
			super(BamRecord.class, IntIntRecord.class);
		}

		@Override
		public void process(BamRecord record) {

			for (int pos = 0; pos < record.getValue().getReadLength(); pos++) {
				outRecord.setKey(pos);
				outRecord.setValue(record.getValue().getBaseQualities()[pos]);
				emit(outRecord);
			}

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Bam Quality Check",
				BamQualityCheck.class);

		pipeline.loadBam(input).apply(SplitByPos.class).mean().save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}