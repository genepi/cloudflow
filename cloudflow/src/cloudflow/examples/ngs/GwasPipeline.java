package cloudflow.examples.ngs;

import genepi.base.Tool;

import java.io.IOException;

import cloudflow.core.Pipeline;
import cloudflow.core.PipelineConf;
import cloudflow.core.operations.Executor;
import cloudflow.core.operations.Filter;
import cloudflow.core.records.TextRecord;

public class GwasPipeline extends Tool {

	// ------------- Execute Snptest

	static public class SnpTestExecutor extends Executor {

		private String binary;
		private String phenotype;
		private String frequentist;
		private String method;
		private String tempPhenoFiles;

		@Override
		public void configure(PipelineConf conf) {
			binary = conf.getFile("snptest");
			tempPhenoFiles = conf.getFile("phenotypes");
			phenotype = conf.get("testPhenotype");
			frequentist = conf.get("frequentist");
			method = conf.get("method");
		}

		@Override
		public boolean execute(String input, String output) {

			SnpTestBinary snpTest = new SnpTestBinary(binary, input, output);
			snpTest.setPhenotype(phenotype);
			snpTest.setFrequent(Integer.parseInt(frequentist));
			snpTest.setMethod(method);
			snpTest.setExcludeList(null);
			snpTest.setPhenotypeFile(tempPhenoFiles);
			return (snpTest.execute() == 0);

		}

	}

	// ------------- Remove Header

	static public class FilterHeader extends Filter<TextRecord> {

		public FilterHeader() {
			super(TextRecord.class);
		}

		@Override
		public boolean filter(TextRecord record) {
			return record.getValue().startsWith("id");
		}
	}

	// ------------- Remove invalid Snps (col23 == -1)

	static public class FilterInvalidSnps extends Filter<TextRecord> {

		public FilterInvalidSnps() {
			super(TextRecord.class);
		}

		@Override
		public boolean filter(TextRecord record) {
			String[] splits = record.getValue().split(" ");
			return splits[22].equals("-1");
		}

	}

	// ----Tool

	public GwasPipeline(String[] args) {
		super(args);
	}

	@Override
	public void createParameters() {
		addParameter("input", "input path");
		addParameter("output", "output path");
		addParameter("pheno-file", "input path");
		addParameter("freq", "input file format");
		addParameter("method", "reference");
		addParameter("pheno", "input path");
	}

	@Override
	public void init() {

	}

	@Override
	public int run() {

		int LINES_PER_CHUNK = 10000;

		String input = (String) getValue("input");
		String output = (String) getValue("output");
		String phenofile = (String) getValue("pheno-file");
		String freq = (String) getValue("freq");
		String method = (String) getValue("method");
		String pheno = (String) getValue("pheno");

		Pipeline pipeline = new Pipeline("GWAS-Pipeline", GwasPipeline.class);
		pipeline.set("testPhenotype", pheno);
		pipeline.set("frequentist", freq);
		pipeline.set("method", method);
		pipeline.distributeFile("phenotypes", phenofile);
		pipeline.distributeFile("snptest", "snptest_v2.4.1");

		pipeline.loadTextAndSplit(input, LINES_PER_CHUNK)
				.execute(SnpTestExecutor.class).apply(FilterHeader.class)
				.apply(FilterInvalidSnps.class).save(output);

		try {
			return pipeline.run() ? 0 : 1;
		} catch (IOException e) {
			e.printStackTrace();
			return 1;
		}

	}

	public static void main(String[] args) {
		new GwasPipeline(args).start();
	}

}
