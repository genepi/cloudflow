package cloudflow.bio.vcf.filters;

import htsjdk.variant.variantcontext.VariantContext;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.operations.Filter;

public class DuplicateFilter extends Filter<VcfRecord> {

	public DuplicateFilter() {
		super(VcfRecord.class);
	}

	@Override
	public boolean filter(VcfRecord record) {
		VariantContext snp = record.getValue();
		return snp.isFiltered() && snp.getFilters().contains("DUP");
	}

}
