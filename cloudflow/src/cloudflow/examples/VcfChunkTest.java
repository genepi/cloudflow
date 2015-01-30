package cloudflow.examples;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.bio.vcf.VcfChunk;
import cloudflow.core.hadoop.RecordValues;
import cloudflow.core.operations.ReduceStep;
import cloudflow.core.records.TextRecord;

public class VcfChunkTest {

	static public class ChunkInfos extends ReduceStep<VcfChunk, TextRecord> {

		private TextRecord info = new TextRecord();

		@Override
		public void process(String key, RecordValues<VcfChunk> values) {
			int noSnps = 0;
			while (values.hasNextRecord()) {
				noSnps++;
				//consume!!
				values.getRecord();
			}

			info.setKey(key);
			info.setValue(noSnps + " SNPS in chunk.");

			emit(info);

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("VCF Chunk test",
				VcfChunkTest.class);

		pipeline.loadVcf(input).createChunks().apply(ChunkInfos.class)
				.save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
