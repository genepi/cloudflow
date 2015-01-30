package cloudflow.examples;

import java.io.IOException;

import cloudflow.core.Pipeline;
import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.io.TextLoader;
import cloudflow.core.operations.Filter;
import cloudflow.core.operations.MapStep;
import cloudflow.core.operations.ReduceStep;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.TextRecord;

public class BioWordCount {

	static public class RemoveHeader extends Filter<TextRecord> {

		@Override
		public boolean filter(TextRecord record) {
			return record.getValue().startsWith("#");
		}

	}

	static public class SplitTiTv extends MapStep<TextRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		@Override
		public void process(TextRecord record) {

			VcfLine vcfLine = new VcfLine(record.getValue());
			for (String titv : vcfLine.getTiTv()) {
				outRecord.setKey(titv);
				outRecord.setValue(1);
				emit(outRecord);
			}

		}

	}

	static public class CountTiTv extends
			ReduceStep<IntegerRecord, IntegerRecord> {

		private IntegerRecord outRecord = new IntegerRecord();

		@Override
		public void process(String key, RecordValues<IntegerRecord> values) {

			int sum = 0;
			while (values.hasNextRecord()) {
				sum += values.getRecord().getValue();
			}
			outRecord.setKey(key);
			outRecord.setValue(sum);
			emit(outRecord);
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("BioWordCount", BioWordCount.class);

		pipeline.load(input, new TextLoader())
				.apply(RemoveHeader.class, TextRecord.class)
				.apply(SplitTiTv.class, IntegerRecord.class).groupByKey()
				.apply(CountTiTv.class).save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
