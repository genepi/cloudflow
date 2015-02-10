package cloudflow.bio.vcf.filters;

import htsjdk.variant.variantcontext.VariantContext;
import cloudflow.bio.vcf.VcfRecord;
import cloudflow.core.operations.Filter;

public class InvalidAlleleFilter extends Filter<VcfRecord> {

	public InvalidAlleleFilter() {
		super(VcfRecord.class);
	}

	@Override
	public boolean filter(VcfRecord record) {
		VariantContext snp = record.getValue();
		//Todo: check alternate allele
		return !isValid(snp.getReference().getBaseString());
		
		
	}

	private boolean isValid(String allele) {
		return allele.toUpperCase().equals("A")
				|| allele.toUpperCase().equals("C")
				|| allele.toUpperCase().equals("G")
				|| allele.toUpperCase().equals("T");
	}

	
}
