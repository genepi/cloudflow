package cloudflow.examples;

import java.io.IOException;

import cloudflow.core.Pipeline;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.io.TextLoader;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.TextRecord;

public class LengthCount {

	static public class SplitByWordLength extends
			Transformer<TextRecord, IntegerRecord> {

		private IntegerRecord outRecord = new IntegerRecord();

		public SplitByWordLength() {
			super(TextRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(TextRecord record) {

			String[] tiles = record.getValue().split(" ");
			for (String tile : tiles) {
				outRecord.setKey(tile.length() + "");
				outRecord.setValue(1);
				emit(outRecord);
			}

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("Wordcount-Length!", LengthCount.class);

		pipeline.load(input, new TextLoader()).apply(SplitByWordLength.class)
				.sum().save(output);

		boolean result = new MapReduceRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
