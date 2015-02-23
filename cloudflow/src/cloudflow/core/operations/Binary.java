package cloudflow.core.operations;

import genepi.hadoop.command.Command;

public class Binary extends Command {

	private String myParams = "";

	private String myCmd = "";

	public Binary(String cmd, String params) {
		super(cmd);
		this.myCmd = cmd;
		this.myParams = params;
	}

	public String getParamString() {
		return myParams;
	}

	public void setParamString(String myParams) {
		this.myParams = myParams;
	}

	public String getFilename() {
		return myCmd;
	}

	public int execute(String... values) {
		String concreteParams = String.format(myParams, values);
		String[] params = concreteParams.split("\\s+");
		setParams(params);
		return execute();
	}

}
