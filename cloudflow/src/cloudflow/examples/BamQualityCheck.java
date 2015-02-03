package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.bam.BamRecord;
import cloudflow.core.operations.MapOperation;
import cloudflow.core.records.IntegerRecord;

public class BamQualityCheck {

	static public class SplitByPos extends MapOperation<BamRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		public SplitByPos() {
			super(BamRecord.class, IntegerRecord.class);
		}

		@Override
		public void process(BamRecord record) {

			for (int pos = 0; pos < record.getValue().getReadLength(); pos++) {
				outRecord.setKey(pos + "");
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
