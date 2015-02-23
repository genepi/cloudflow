package cloudflow.bio.vcf;

import cloudflow.core.PipelineConf;
import cloudflow.core.operations.Transformer;

public class VcfChunker extends Transformer<VcfRecord, VcfChunk> {

	private VcfChunk chunk = new VcfChunk();

	public static int DEFAULT_CHUNK_SIZE = 10000000;

	private int chunkSize = DEFAULT_CHUNK_SIZE;

	public VcfChunker() {
		super(VcfRecord.class, VcfChunk.class);
	}

	@Override
	public void configure(PipelineConf conf) {
		String sizeValue = conf.get("chunker.vcf.size");
		if (sizeValue != null) {
			chunkSize = Integer.parseInt(sizeValue);
		}
	}

	@Override
	public void transform(VcfRecord record) {

		int chunkNr = record.getValue().getStart() / chunkSize;

		chunk.setChr(record.getValue().getChr());
		chunk.setStart(chunkNr * chunkSize);
		chunk.setEnd(((chunkNr + 1) * chunkSize) - 1);
		chunk.setValue(record.getValue());

		emit(chunk);
	}

}
