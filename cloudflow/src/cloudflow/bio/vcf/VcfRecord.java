package cloudflow.bio.vcf;

import htsjdk.variant.variantcontext.VariantContext;
import cloudflow.core.records.Record;

public class VcfRecord extends Record<String, VariantContext> {

	private String key;

	private VariantContext value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public VariantContext getValue() {
		return value;
	}

	public void setValue(VariantContext variantContext) {
		this.value = value;
	}

}
