package cloudflow.hadoop;

public class SnpTest {

	public static void main(String[] args) {

		String input = "";
		String output = "";

		final Binary snptest = new Binary("snptest");

		Pipeline pipeline = new Pipeline("Snpt-Test Pipeline");
		pipeline.distribute(snptest);
		//pipeline.load(input, VCFLoader.class);
		pipeline.load(input);
		
		pipeline.split(VCFSplitter.class); //Mapper!!
		pipeline.filter(new IFilter() {

		@Override
		public boolean process(String line) {

			InputFile input = InputFile.fillWith(line);
			OutputFile output = new OutputFile();

			snptest.execute("--input", input.getFilename(), "--output",
					output.getFilename());

			return true;
		}
		
		
	});
		
		// pipeline.groupBy(new IGroupBy(){...});
		pipeline.forEachChunk(new IForEach() 
		@Override
		public String process(List<String> lines) {
			
			
		}

		pipeline.forEachLine(new IForEach() {

			@Override
			public String process(String line) {

				InputFile input = InputFile.fillWith(line);
				OutputFile output = new OutputFile();

				snptest.execute("--input", input.getFilename(), "--output",
						output.getFilename());

				return output.getContent();
			}
			
			
		});
		//pipeline.filter(new IFilter(){...});

		// pipeline.groupBy();

		pipeline.save(output);

	}
}
