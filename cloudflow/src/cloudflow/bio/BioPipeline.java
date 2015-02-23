package cloudflow.bio;

import cloudflow.bio.bam.BamLoader;
import cloudflow.bio.bam.CountBasesPerPosition;
import cloudflow.bio.bam.FindVariants;
import cloudflow.bio.fastq.Aligner;
import cloudflow.bio.fastq.CreateFastqPairs;
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

		public ReduceBuilder split() {
			return apply(VcfChunker.class).groupByKey();
		}

		public ReduceBuilder split(int size, ChunkSize type) {
			set("chunker.vcf.size", size * type.getSize());
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

		public AfterReduceBuilder findVariations(String reference) {
			pipeline.distributeArchive("reference.tar.gz", reference);
			return apply(CountBasesPerPosition.class).groupByKey().apply(
					FindVariants.class);
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

		public FastqReduceBuilder findPairs() {
			apply(CreateFastqPairs.class);
			return new FastqReduceBuilder(pipeline);
		}

		// paired reads? mapper seppi mtdna-server
	}

	public class FastqReduceBuilder extends ReduceBuilder {

		public FastqReduceBuilder(Pipeline pipeline) {
			super(pipeline);
		}

		public AfterReduceBuilder align(String reference) {
			pipeline.distributeArchive("jbwa.tar.gz", "jbwa075a.tar.gz");
			pipeline.distributeArchive("reference.tar.gz", reference);
			return apply(Aligner.class);
		}

	}

}
