package cloudflow.hadoop.test;

import java.util.List;

public interface IMapStep {

	public List<Record>  process(List<Record> records);

}
