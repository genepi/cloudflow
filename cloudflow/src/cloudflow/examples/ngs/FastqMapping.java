package cloudflow.examples.ngs;

import java.io.IOException;

import cloudflow.bio.BioPipeline;
import cloudflow.core.hadoop.MapReduceRunner;

public class FastqMapping {

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Bwa-MEM-Mapper",
				FastqMapping.class);

		pipeline.loadFastq(input).findPairs().align("rcrs.tar.gz").save(output);

		boolean result = new MapReduceRunner().run(pipeline);
		if (!result) {
			System.exit(1);
		}
	}
}
