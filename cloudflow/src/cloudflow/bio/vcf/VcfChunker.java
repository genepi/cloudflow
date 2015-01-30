package cloudflow.bio.vcf;

import cloudflow.core.operations.MapStep;

public class VcfChunker extends MapStep<VcfRecord, VcfChunk> {

	private VcfChunk chunk = new VcfChunk();

	public static int CHUNK_SIZE = 10000000;

	@Override
	public void process(VcfRecord record) {

		int chunkNr = record.getValue().getStart() / CHUNK_SIZE;

		chunk.setChr(record.getValue().getChr());
		chunk.setStart(chunkNr * CHUNK_SIZE);
		chunk.setEnd(((chunkNr + 1) * CHUNK_SIZE) - 1);
		chunk.setValue(record.getValue());

		createRecord(chunk);
	}

}
