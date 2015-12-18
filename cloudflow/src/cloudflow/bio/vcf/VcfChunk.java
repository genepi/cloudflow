package cloudflow.bio.vcf;

import htsjdk.variant.variantcontext.VariantContext;
import cloudflow.core.records.Record;

public class VcfChunk extends Record<String, VariantContext> {

	private String key;
	
	private VariantContext value;
	
	private String chr = "";

	private int start = 0;

	private int end = 0;

	public void setKey(String key) {
		this.key = key;
	}
	public String getKey() {
		return key;
	}
	public void setValue(VariantContext value) {
		this.value = value;
	}
	public VariantContext getValue() {
		return value;
	}
	
	public String getChr() {
		return chr;
	}

	public void setChr(String chr) {
		this.chr = chr;
		setKey(chr + ":" + start + "-" + end);
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
		setKey(chr + ":" + start + "-" + end);
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
		setKey(chr + ":" + start + "-" + end);
	}

}
