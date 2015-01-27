package cloudflow.hadoop.test;

import java.util.List;

public interface IReduceStep {

	public List<Record> process(List<Record> line);

}
