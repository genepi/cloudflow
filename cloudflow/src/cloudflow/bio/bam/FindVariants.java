package cloudflow.bio.bam;

import java.io.File;

import cloudflow.bio.util.ReferenceUtil;
import cloudflow.core.PipelineConf;
import cloudflow.core.hadoop.GroupedRecords;
import cloudflow.core.operations.Summarizer;
import cloudflow.core.records.TextRecord;

public class FindVariants extends Summarizer<BasePositionRecord, TextRecord> {

	private BasePosition sum = new BasePosition();

	private String reference;

	private TextRecord output = new TextRecord();

	public FindVariants() {
		super(BasePositionRecord.class, TextRecord.class);
	}

	@Override
	public void configure(PipelineConf conf) {

		String referencePath = conf.getArchive("reference.tar.gz");
		File referenceFile = new File(referencePath);
		String refString = ReferenceUtil.findFileinReferenceArchive(
				referenceFile, ".fasta");
		reference = ReferenceUtil.readInReference(refString);
	}

	@Override
	public void summarize(String key, GroupedRecords<BasePositionRecord> values) {

		int position = Integer.parseInt(key);

		// sum up
		sum.clear();
		while (values.hasNextRecord()) {
			sum.add(values.getRecord().getValue());
		}

		if (position > 0 && position <= reference.length()) {
			char topBase = sum.getTopBase();
			char refBase = reference.charAt(position - 1);
			if (topBase != refBase) {
				output.setKey(key);
				output.setValue("BASE: " + topBase + ", REF: " + refBase);
				emit(output);
			}
		}

	}

}
