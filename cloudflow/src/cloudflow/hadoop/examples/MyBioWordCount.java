package cloudflow.hadoop.examples;

import java.io.IOException;

import cloudflow.hadoop.records.Record;
import cloudflow.hadoop.records.RecordValues;
import cloudflow.hadoop.test.MapStep;
import cloudflow.hadoop.test.Pipeline;
import cloudflow.hadoop.test.RecordFilter;
import cloudflow.hadoop.test.ReduceStep;
import cloudflow.hadoop.test.TextLoader;

public class MyBioWordCount {

	static public class RemoveHeader extends RecordFilter {

		@Override
		public boolean filter(Record record) {
			return record.getValue().startsWith("#");
		}

	}

	static public class SplitTiTv extends MapStep {

		String one = "1";

		@Override
		public void process(Record record) {

			VcfLine vcfLine = new VcfLine(record.getValue());
			for (String titv : vcfLine.getTiTv()) {
				createRecord(titv, one);
			}

		}

	}

	static public class CountTiTv extends ReduceStep {

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

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("BioWordCount", MyBioWordCount.class);

		pipeline.load(input, new TextLoader());

		pipeline.perform(RemoveHeader.class).perform(SplitTiTv.class)
				.groupByKey().perform(CountTiTv.class);

		pipeline.save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
