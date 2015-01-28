package cloudflow.hadoop.test;

import java.util.List;

public abstract class ReduceStep {

	public abstract List<Record> process(List<Record> line);

}
