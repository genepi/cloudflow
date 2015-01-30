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

public class WordCount {

	static public class SplitWords extends MapStep<TextRecord, IntegerRecord> {

		private IntegerRecord outRecord = new IntegerRecord();

		public SplitWords() {
			super(TextRecord.class, IntegerRecord.class);
		}

		@Override
		public void process(TextRecord record) {

			String[] tiles = record.getValue().split(" ");
			for (String tile : tiles) {
				outRecord.setKey(tile);
				outRecord.setValue(1);
				emit(outRecord);
			}

		}

	}

	static public class RemoveEmptyKeys extends Filter<IntegerRecord> {

		public RemoveEmptyKeys() {
			super(IntegerRecord.class);
		}

		@Override
		public boolean filter(IntegerRecord record) {
			return record.getKey().trim().isEmpty();
		}

	}

	static public class CountWords extends
			ReduceStep<IntegerRecord, IntegerRecord> {

		private IntegerRecord outRecord = new IntegerRecord();

		public CountWords() {
			super(IntegerRecord.class, IntegerRecord.class);
		}

		@Override
		public void process(String key, RecordValues<IntegerRecord> values) {

			int sum = 0;
			while (values.hasNextRecord()) {
				int intValue = values.getRecord().getValue();
				sum += intValue;
			}
			outRecord.setKey(key);
			outRecord.setValue(sum);
			emit(outRecord);
		}

	}

	static public class FilterWords extends Filter<IntegerRecord> {

		public FilterWords() {
			super(IntegerRecord.class);
		}

		@Override
		public boolean filter(IntegerRecord record) {
			return record.getValue() < 100;
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("Wordcount", WordCount.class);

		pipeline.load(input, new TextLoader()).apply(SplitWords.class)
				.apply(RemoveEmptyKeys.class).groupByKey()
				.apply(CountWords.class).perform(FilterWords.class)
				.save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
