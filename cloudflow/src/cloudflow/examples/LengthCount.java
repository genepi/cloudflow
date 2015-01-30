package cloudflow.examples;

import java.io.IOException;

import cloudflow.core.Pipeline;
import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.io.TextLoader;
import cloudflow.core.operations.MapStep;
import cloudflow.core.operations.ReduceStep;
import cloudflow.core.records.IntIntRecord;
import cloudflow.core.records.TextRecord;

public class LengthCount {

	static public class SplitByWordLength extends
			MapStep<TextRecord, IntIntRecord> {

		private IntIntRecord outRecord = new IntIntRecord();

		public SplitByWordLength() {
			super(TextRecord.class, IntIntRecord.class);
		}

		@Override
		public void process(TextRecord record) {

			String[] tiles = record.getValue().split(" ");
			for (String tile : tiles) {
				outRecord.setKey(tile.length());
				outRecord.setValue(1);
				emit(outRecord);
			}

		}

	}

	static public class CountWordLength extends
			ReduceStep<IntIntRecord, IntIntRecord> {

		private IntIntRecord outRecord = new IntIntRecord();

		public CountWordLength() {
			super(IntIntRecord.class, IntIntRecord.class);
		}

		@Override
		public void process(String key, RecordValues<IntIntRecord> values) {

			int sum = 0;
			while (values.hasNextRecord()) {
				int intValue = values.getRecord().getValue();
				sum += intValue;
			}
			outRecord.setKey(Integer.parseInt(key));
			outRecord.setValue(sum);
			emit(outRecord);
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("Wordcount-Length!", LengthCount.class);

		pipeline.load(input, new TextLoader()).apply(SplitByWordLength.class)
				.groupByKey().apply(CountWordLength.class).save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
