package cloudflow.core;

import java.io.IOException;

public abstract class PipelineRunner {

	public abstract boolean run(Pipeline pipeline) throws IOException;

}
