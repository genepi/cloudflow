package cloudflow.hadoop;

import java.io.IOException;
import java.util.List;

import cloudflow.hadoop.test.IMapStep;
import cloudflow.hadoop.test.IReduceStep;
import cloudflow.hadoop.test.Pipeline;
import cloudflow.hadoop.test.Record;
import cloudflow.hadoop.test.TextLoader;

public class SnptestTest {

	static public class CreateChunks implements IMapStep {

		@Override
		public List<Record> process(List<Record> records) {
			
			//create chunks based on position (?)
			
			return null;
			
		}

	}

	static public class ExecuteSnpTest implements IReduceStep {

		@Override
		public List<Record> process(List<Record> records) {

			//write records to input file (one record, one line)
			
			//execute snptest
			
			//create records from output file (one line, one record)
			
			return null;

		}

	}

	static public class FilterResults implements IReduceStep {

		@Override
		public List<Record> process(List<Record> records) {

			//filter records with 25 columns or -1 in last column
			//filter header
			
			return null;

		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("SnpTest", SnptestTest.class);
		pipeline.load(input, new TextLoader());

		pipeline.addMapStep(CreateChunks.class);
		pipeline.addReduceStep(ExecuteSnpTest.class);
		pipeline.addReduceStep(FilterResults.class);

		pipeline.save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
