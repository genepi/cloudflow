package cloudflow.examples;

import java.io.IOException;

import cloudflow.core.Pipeline;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.io.TextLoader;
import cloudflow.core.operations.Filter;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.TextRecord;

public class BioWordCount {

	static public class RemoveHeader extends Filter<TextRecord> {

		public RemoveHeader() {
			super(TextRecord.class);
		}

		@Override
		public boolean filter(TextRecord record) {
			return record.getValue().startsWith("#");
		}

	}

	static public class SplitTiTv extends
			Transformer<TextRecord, IntegerRecord> {

		IntegerRecord outRecord = new IntegerRecord();

		public SplitTiTv() {
			super(TextRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(TextRecord record) {

			VcfLine vcfLine = new VcfLine(record.getValue());
			for (String titv : vcfLine.getTiTv()) {
				outRecord.setKey(titv);
				outRecord.setValue(1);
				emit(outRecord);
			}

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("BioWordCount", BioWordCount.class);

		pipeline.load(input, new TextLoader()).apply(RemoveHeader.class)
				.apply(SplitTiTv.class).sum().save(output);

		boolean result = new MapReduceRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
