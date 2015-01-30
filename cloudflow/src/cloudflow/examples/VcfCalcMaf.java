package cloudflow.examples;

import htsjdk.variant.variantcontext.VariantContext;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.operations.Filter;
import cloudflow.core.operations.MapStep;
import cloudflow.core.records.StringFloatRecord;

public class VcfCalcMaf {

	static public class CalcMaf extends MapStep<VcfRecord, StringFloatRecord> {

		StringFloatRecord outRecord = new StringFloatRecord();

		@Override
		public void process(VcfRecord record) {

			VariantContext snp = record.getValue();
			float maf = calculateMaf(snp);

			outRecord.setKey(snp.getChr() + ":" + snp.getStart());
			outRecord.setValue(maf);
			createRecord(outRecord);

		}

		private float calculateMaf(VariantContext snp) {
			float maf = (snp.getHetCount() + snp.getHomRefCount())
					/ (float) (snp.getHetCount() + snp.getHomRefCount() + snp
							.getHomVarCount());

			if (maf > 0.5) {
				maf = 1 - maf;
			}
			return maf;
		}

	}

	static public class FilterCommonSnps extends Filter<StringFloatRecord> {

		@Override
		public boolean filter(StringFloatRecord record) {
			return record.getValue() > 0.05 || record.getValue() == 0;
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Calc MAF", VcfCalcMaf.class);

		pipeline.loadVcf(input).perform(CalcMaf.class, StringFloatRecord.class)
				.perform(FilterCommonSnps.class, StringFloatRecord.class)
				.save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
