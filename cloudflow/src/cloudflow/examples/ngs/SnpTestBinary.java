package cloudflow.examples.ngs;

import genepi.hadoop.command.Command;

import java.util.ArrayList;
import java.util.List;

public class SnpTestBinary extends Command {

	private String input;

	private String output;

	private String phenotypeFile;

	private String singlePhenotype;

	private String method;

	private String excludeList;

	private int frequentist;

	public SnpTestBinary(String path, String input, String output) {
		super(path);
		this.input = input;
		this.output = output;

	}

	@Override
	public int execute() {

		List<String> params = new ArrayList<String>();

		params.add("-data");
		params.add(input);
		params.add(phenotypeFile);
		params.add("-o");
		params.add(output);
		params.add("-frequentist");
		params.add(String.valueOf(frequentist));
		params.add("-method");
		params.add(method);
		params.add("-pheno");
		params.add(singlePhenotype);
		params.add("-use_raw_phenotypes");
		params.add("-hwe");
		if (excludeList != null && !excludeList.isEmpty()) {
			params.add("-exclude_samples");
			params.add(excludeList);
		}

		setParams(params);

		return super.execute();
	}

	public String getPhenotype() {
		return singlePhenotype;
	}

	public void setPhenotype(String phenotype) {
		this.singlePhenotype = phenotype;
	}

	public void setFrequent(int frequentist) {
		this.frequentist = frequentist;
	}

	public int getFrequent() {
		return frequentist;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getMethod() {
		return method;
	}

	public void setPhenotypeFile(String phenotypeFile) {
		this.phenotypeFile = phenotypeFile;
	}

	public String getPhenotypeFile() {
		return phenotypeFile;
	}

	public void setExcludeList(String excludeList) {
		this.excludeList = excludeList;
	}

	public String getExcludeList() {
		return excludeList;
	}

}