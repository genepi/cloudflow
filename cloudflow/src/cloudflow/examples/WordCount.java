package cloudflow.examples;

import java.io.IOException;
import java.util.Set;

import cloudflow.core.Pipeline;
import cloudflow.core.operations.Filter;
import cloudflow.core.operations.Transformer;
import cloudflow.core.records.IntegerRecord;
import cloudflow.core.records.TextRecord;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

public class WordCount {

	static public class LineToWords extends
			Transformer<TextRecord, IntegerRecord> {

		private static final Splitter SPLITTER = Splitter.onPattern("\\s+")
				.omitEmptyStrings();

		private IntegerRecord outRecord = new IntegerRecord();

		public LineToWords() {
			super(TextRecord.class, IntegerRecord.class);
		}

		@Override
		public void transform(TextRecord record) {
			for (String word : SPLITTER.split(record.getValue())) {
				outRecord.setKey(word);
				outRecord.setValue(1);
				emit(outRecord);
			}

		}

	}

	static public class RemoveEmptyKeys extends Filter<IntegerRecord> {

		// English stop words, borrowed from Lucene.
		private static final Set<String> STOP_WORDS = ImmutableSet
				.copyOf(new String[] { "a", "and", "are", "as", "at", "be",
						"but", "by", "for", "if", "in", "into", "is", "it",
						"no", "not", "of", "on", "or", "s", "such", "t",
						"that", "the", "their", "then", "there", "these",
						"they", "this", "to", "was", "will", "with" });

		public RemoveEmptyKeys() {
			super(IntegerRecord.class);
		}

		@Override
		public boolean filter(IntegerRecord record) {
			return STOP_WORDS.contains(record.getValue());
		}

	}

	static public class FilterWords extends Filter<IntegerRecord> {

		public FilterWords() {
			super(IntegerRecord.class);
		}

		@Override
		public boolean filter(IntegerRecord record) {
			return record.getValue() < 100;
		}

	}

	public static void main(String[] args) throws IOException {

		String input = args[0];
		String output = args[1];

		Pipeline pipeline = new Pipeline("Wordcount", WordCount.class);

		pipeline.loadText(input).apply(LineToWords.class)
				.filter(RemoveEmptyKeys.class).sum().save(output);

		boolean result = pipeline.run();
		if (!result) {
			System.exit(1);
		}
	}
}
