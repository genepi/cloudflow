package cloudflow.bio.vcf;

import htsjdk.variant.variantcontext.VariantContext;

import org.apache.hadoop.io.IntWritable;
import org.seqdoop.hadoop_bam.VariantContextWritable;

import cloudflow.core.records.Record;

public class VcfRecord extends Record<IntWritable, VariantContextWritable> {

	public VcfRecord() {
		setWritableKey(new IntWritable());
		setWritableValue(new VariantContextWritable());
	}

	public VariantContext getValue() {
		return getWritableValue().get();
	}

	public void setValue(VariantContext value) {
		getWritableValue().set(value);
	}

	public int getKey() {
		return getWritableKey().get();
	}

	public void setKey(int key) {
		getWritableKey().set(key);
	}
}
