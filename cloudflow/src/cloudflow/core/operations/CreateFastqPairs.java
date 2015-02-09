package cloudflow.core.operations;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;
import org.seqdoop.hadoop_bam.SequencedFragment;

import cloudflow.bio.fastq.FastqRecord;
import cloudflow.bio.fastq.SingleRead;
import cloudflow.core.PipelineConf;
import cloudflow.core.records.ShortReadRecord;
import cloudflow.core.records.TextRecord;

public class CreateFastqPairs extends MapOperation<FastqRecord, ShortReadRecord> {

	ShortReadRecord outRecord = new ShortReadRecord();

	public CreateFastqPairs() {
		super(FastqRecord.class, ShortReadRecord.class);

	}


	@Override
	public void process(FastqRecord record) {

		Text key = new Text(record.getKey().toString());
		Text outKey = new Text();
		SequencedFragment value = record.getValue();

		// reset builder
		StringBuilder builder = new StringBuilder();
		String seq;
		SingleRead read = new SingleRead();
		String qual;
		builder.delete(0, builder.length());
		seq = value.getSequence().toString();
		qual = value.getQuality().toString();

		/**
		 * FASTQ format with /1 and /2 at the end
		 * 
		 * @SRR062634.1 HWI-EAS110_103327062:6:1:1092:8469/1
		 */
		if (key.toString().charAt(key.getLength() - 2) == '/') {

			try {
				outKey.set(Text.decode(key.getBytes(), 0, key.getLength() - 1));
			} catch (CharacterCodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		/**
		 * new FASTQ file format
		 * 
		 * @HWI-ST301L:236:C0EJ5ACXX:1:1101:1436:2180 1:N:0:ATCACG
		 */
		else {

			builder = generateFastqKey(builder, value);
			outKey.set(builder.toString());

		}

		read.setReadLength(seq.length());
		read.setName(outKey.toString());
		read.setBases(seq.getBytes());
		read.setQual(qual.getBytes());
		read.setFilename("TestfileXXX");
		read.setReadNumber(value.getRead());

		outRecord.setWritableKey(outKey);
		outRecord.setWritableValue(read);
		emit(outRecord);

	}

	/** SEAL preprocessing */
	protected StringBuilder generateFastqKey(StringBuilder builder,
			SequencedFragment read) {

		builder.append(read.getInstrument() == null ? "" : read.getInstrument());
		builder.append(":").append(
				read.getRunNumber() == null ? "" : read.getRunNumber());
		builder.append(":").append(
				read.getFlowcellId() == null ? "" : read.getFlowcellId());
		builder.append(":").append(read.getLane());
		builder.append(":").append(read.getTile());
		builder.append(":").append(read.getXpos());
		builder.append(":").append(read.getYpos());

		return builder;

	}
}