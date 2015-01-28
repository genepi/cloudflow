package cloudflow.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import cloudflow.hadoop.old.DistributedBinary;
import cloudflow.hadoop.test.MapStep;
import cloudflow.hadoop.test.ReduceStep;
import cloudflow.hadoop.test.Pipeline;
import cloudflow.hadoop.test.Record;
import cloudflow.hadoop.test.TextLoader;

public class WordCountTest {

	static public class SplitWords extends MapStep {

		@Override
		public List<Record> process(List<Record> records) {

			List<Record> words = new Vector<Record>();
			for (Record record : records) {
				String[] tiles = record.getValue().split(" ");
				for (String tile : tiles) {

					words.add(new Record(tile, "1"));
				}
			}
			return words;
		}

	}

	static public class RemoveEmptyKeys extends MapStep {

		@Override
		public List<Record> process(List<Record> records) {
			List<Record> count = new Vector<Record>();
			for (Record record : records) {
				if (!record.getKey().trim().isEmpty()) {
					count.add(record);
				}
			}
			return count;
		}

	}

	static public class CountWords extends ReduceStep {

		@Override
		public List<Record> process(List<Record> records) {
			List<Record> count = new Vector<Record>();
			int sum = 0;
			for (Record record : records) {
				int intValue = Integer.parseInt(record.getValue());
				sum += intValue;
			}
			count.add(new Record(records.get(0).getKey(), sum + ""));
			return count;
		}

	}

	static public class FilterWords extends ReduceStep {

		@Override
		public List<Record> process(List<Record> records) {
			List<Record> count = new Vector<Record>();
			for (Record record : records) {
				int sum = Integer.parseInt(record.getValue());
				if (sum > 100) {
					count.add(record);
				}
			}
			return count;
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		DistributedBinary snptest = new DistributedBinary("snptest");

		Pipeline pipeline = new Pipeline("Wordcount", WordCountTest.class);
		pipeline.distribute("snptest", snptest);

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
