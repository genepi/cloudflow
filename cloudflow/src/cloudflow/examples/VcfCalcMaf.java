package cloudflow.examples;

import htsjdk.variant.variantcontext.VariantContext;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.hadoop.MapReduceRunner;
import cloudflow.core.operations.Filter;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.FloatRecord;

public class VcfCalcMaf {

	static public class CalcMaf extends Transformer<VcfRecord, FloatRecord> {

		FloatRecord outRecord = new FloatRecord();

		public CalcMaf() {
			super(VcfRecord.class, FloatRecord.class);
		}

		@Override
		public void transform(VcfRecord record) {

			VariantContext snp = record.getValue();
			float maf = calculateMaf(snp);

			outRecord.setKey(snp.getChr() + ":" + snp.getStart());
			outRecord.setValue(maf);
			emit(outRecord);

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

	static public class FilterCommonSnps extends Filter<FloatRecord> {

		public FilterCommonSnps() {
			super(FloatRecord.class);
		}

		@Override
		public boolean filter(FloatRecord record) {
			return record.getValue() > 0.05 || record.getValue() == 0;
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Calc MAF", VcfCalcMaf.class);

		pipeline.loadVcf(input).apply(CalcMaf.class)
				.apply(FilterCommonSnps.class).save(output);

		boolean result = new MapReduceRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
