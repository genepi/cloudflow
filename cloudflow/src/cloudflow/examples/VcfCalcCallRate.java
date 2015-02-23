package cloudflow.examples;

import htsjdk.variant.variantcontext.VariantContext;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.FloatRecord;

public class VcfCalcCallRate {

	static public class CalcCallRate extends
			Transformer<VcfRecord, FloatRecord> {

		FloatRecord outRecord = new FloatRecord();

		public CalcCallRate() {
			super(VcfRecord.class, FloatRecord.class);
		}

		@Override
		public void transform(VcfRecord record) {

			VariantContext snp = record.getValue();
			float call = 1 - (snp.getNoCallCount() / (float) snp.getNSamples());
			outRecord.setKey(snp.getID());
			outRecord.setValue(call);
			emit(outRecord);

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("calc call rate",
				VcfCalcCallRate.class);

		pipeline.loadVcf(input).apply(CalcCallRate.class).save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
