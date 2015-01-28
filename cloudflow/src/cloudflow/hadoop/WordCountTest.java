package cloudflow.hadoop;

import java.io.IOException;

import cloudflow.hadoop.old.DistributedBinary;
import cloudflow.hadoop.records.Record;
import cloudflow.hadoop.records.RecordValues;
import cloudflow.hadoop.test.MapStep;
import cloudflow.hadoop.test.Pipeline;
import cloudflow.hadoop.test.RecordFilter;
import cloudflow.hadoop.test.ReduceStep;
import cloudflow.hadoop.test.TextLoader;

public class WordCountTest {

	static public class SplitWords extends MapStep {

		@Override
		public void process(Record record) {

			String[] tiles = record.getValue().split(" ");
			for (String tile : tiles) {
				createRecord(tile, "1");
			}

		}

	}

	static public class RemoveEmptyKeys extends RecordFilter {

		@Override
		public boolean filter(Record record) {
			return record.getKey().trim().isEmpty();
		}

	}

	static public class CountWords extends ReduceStep {

		@Override
		public void process(String key, RecordValues values) {

			int sum = 0;
			while (values.next()) {
				int intValue = Integer.parseInt(values.value());
				sum += intValue;
			}
			createRecord(key, sum + "");
		}

	}

	static public class FilterWords extends RecordFilter {

		@Override
		public boolean filter(Record record) {
			int sum = Integer.parseInt(record.getValue());
			return sum < 100;
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		DistributedBinary snptest = new DistributedBinary("snptest");

		Pipeline pipeline = new Pipeline("Wordcount", WordCountTest.class);

		pipeline.load(input, new TextLoader());

		pipeline.perform(SplitWords.class).perform(RemoveEmptyKeys.class)
				.groupByKey().perform(CountWords.class)
				.perform(FilterWords.class);

		pipeline.save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
