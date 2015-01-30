package cloudflow.bio.vcf;

import htsjdk.variant.variantcontext.VariantContext;

import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import cloudflow.core.records.Record;

public class VcfChunk extends Record<Text, VariantContextWritable> {

	private String chr = "";

	private int start = 0;

	private int end = 0;

	public VcfChunk() {
		setWritableKey(new Text());
		setWritableValue(new VariantContextWritable());
	}

	public VariantContext getValue() {
		return getWritableValue().get();
	}

	public void setValue(VariantContext value) {
		getWritableValue().set(value);
	}

	public String getChr() {
		return chr;
	}

	public void setChr(String chr) {
		this.chr = chr;
		getWritableKey().set(chr + ":" + start + "-" + end);
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
		getWritableKey().set(chr + ":" + start + "-" + end);
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
		getWritableKey().set(chr + ":" + start + "-" + end);
	}

}
