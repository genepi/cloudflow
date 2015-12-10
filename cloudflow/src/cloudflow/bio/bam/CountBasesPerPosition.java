package cloudflow.bio.bam;

import htsjdk.samtools.SAMRecord;
import cloudflow.core.operations.Transformer;

public class CountBasesPerPosition extends
		Transformer<BamRecord, BasePositionRecord> {

	private BasePositionRecord output = new BasePositionRecord();

	public CountBasesPerPosition() {
		super(BamRecord.class, BasePositionRecord.class);
	}

	@Override
	public void transform(BamRecord record) {

		SAMRecord samRecord = record.getValue();

		String readString = samRecord.getReadString();

		for (int i = 0; i < readString.length(); i++) {

			int position = samRecord.getReferencePositionAtReadPosition(i + 1);

			if (samRecord.getBaseQualities()[i] >= 30) {

				BasePosition basePos = output.getValue();
				basePos.clear();
				
				char base = readString.charAt(i);
				switch (base) {
				case 'A':
					basePos.setA(1);
					break;
				case 'C':
					basePos.setC(1);
					break;
				case 'G':
					basePos.setG(1);
					break;
				case 'T':
					basePos.setT(1);
					break;
				case 'N':
					basePos.setN(1);
					break;
				default:
					break;
				}

				output.setKey(position);
				output.setValue(basePos);
				emit(output);

			}

		}

	}

}
