package cloudflow.bio.vcf.filters;

import htsjdk.variant.variantcontext.VariantContext;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.operations.Filter;

public class MonomorphicFilter extends Filter<VcfRecord> {

	public MonomorphicFilter() {
		super(VcfRecord.class);
	}

	@Override
	public boolean filter(VcfRecord record) {
		VariantContext snp = record.getValue();
		return snp.isMonomorphicInSamples()
				|| (snp.getHetCount() == 2 * (snp.getNSamples() - snp
						.getNoCallCount()));
	}

}
