package cloudflow.bio;

import cloudflow.bio.bam.BamLoader;
import cloudflow.bio.fastq.FastqLoader;
import cloudflow.bio.vcf.VcfChunker;
import cloudflow.bio.vcf.VcfLoader;
import cloudflow.core.Pipeline;

public class BioPipeline extends Pipeline {

	public BioPipeline(String name, Class<?> driverClass) {
		super(name, driverClass);
	}

	// --- VCF ---

	public VcfMapBuilder loadVcf(String hdfs) {
		load(hdfs, new VcfLoader());
		return new VcfMapBuilder(this);
	}

	public class VcfMapBuilder extends MapBuilder {

		public VcfMapBuilder(Pipeline pipeline) {
			super(pipeline);
		}

		public ReduceBuilder createChunks() {
			return apply(VcfChunker.class).groupByKey();
		}
		
		public ReduceBuilder createChunks(int size) {
			set("chunker.vcf.size", size);
			return apply(VcfChunker.class).groupByKey();
		}		

	}

	// --- BAM --

	public BamMapBuilder loadBam(String hdfs) {
		load(hdfs, new BamLoader());
		return new BamMapBuilder(this);
	}

	public class BamMapBuilder extends MapBuilder {

		public BamMapBuilder(Pipeline pipeline) {
			super(pipeline);
		}

	}

	// -- FastQ --
	public FastqMapBuilder loadFastq(String hdfs) {
		load(hdfs, new FastqLoader());
		return new FastqMapBuilder(this);
	}

	public class FastqMapBuilder extends MapBuilder {

		public FastqMapBuilder(Pipeline pipeline) {
			super(pipeline);
		}

		// paired reads? mapper seppi mtdna-server
	}
	
}
