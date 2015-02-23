package cloudflow.examples.ngs;

import java.io.IOException;

import cloudflow.bio.BioPipeline;

public class VariantCalling {

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		BioPipeline pipeline = new BioPipeline("Variant Calling",
				VariantCalling.class);

		pipeline.loadBam(input).findVariations("rcrs.tar.gz").save(output);;

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
