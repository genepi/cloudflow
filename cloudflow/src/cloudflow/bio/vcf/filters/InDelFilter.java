package cloudflow.bio.vcf.filters;

import htsjdk.variant.variantcontext.VariantContext;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.operations.Filter;

public class InDelFilter extends Filter<VcfRecord> {

	public InDelFilter() {
		super(VcfRecord.class);
	}

	@Override
	public boolean filter(VcfRecord record) {
		VariantContext snp = record.getValue();
		return snp.isIndel() || snp.isComplexIndel();
	}

}
