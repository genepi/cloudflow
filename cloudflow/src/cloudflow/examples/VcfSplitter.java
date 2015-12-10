package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.TextRecord;

public class VcfSplitter {

	static public class SplitByChr extends Transformer<VcfRecord, TextRecord> {

		TextRecord outRecord = new TextRecord();

		public SplitByChr() {
			super(VcfRecord.class, TextRecord.class);
		}

		@Override
		public void transform(VcfRecord record) {

			String chr = record.getValue().getChr();

			outRecord.setKey(chr);
			outRecord.setValue(record.getValue().toStringDecodeGenotypes());
			emit(outRecord);

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("VCF-Chr-Split", VcfSplitter.class);

		pipeline.loadVcf(input).apply(SplitByChr.class).concat().save(output);

		boolean result = new MapReduceRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
