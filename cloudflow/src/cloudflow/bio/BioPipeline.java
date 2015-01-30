package cloudflow.bio;

import cloudflow.bio.bam.BamLoader;
import cloudflow.bio.vcf.VcfChunk;
import cloudflow.bio.vcf.VcfChunker;
import cloudflow.bio.vcf.VcfLoader;
import cloudflow.core.Pipeline;

public class BioPipeline extends Pipeline {

	public BioPipeline(String name, Class<?> driverClass) {
		super(name, driverClass);
	}

	public VcfMapBuilder loadVcf(String hdfs) {
		load(hdfs, new VcfLoader());
		return new VcfMapBuilder(this);
	}

	public class VcfMapBuilder extends MapBuilder {

		public VcfMapBuilder(Pipeline pipeline) {
			super(pipeline);
		}

		public ReduceBuilder createChunks() {
			return apply(VcfChunker.class, VcfChunk.class).groupByKey();
		}

	}

	public BamMapBuilder loadBam(String hdfs) {
		load(hdfs, new BamLoader());
		return new BamMapBuilder(this);
	}

	public class BamMapBuilder extends MapBuilder {

		public BamMapBuilder(Pipeline pipeline) {
			super(pipeline);
		}

	}

}
