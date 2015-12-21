package cloudflow.examples;

import java.util.Arrays;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.collect.ImmutableSet;

public class SparkWordCount {
	


		// English stop words, borrowed from Lucene.
		public static final Set<String> STOP_WORDS = ImmutableSet
				.copyOf(new String[] { "a", "and", "are", "as", "at", "be",
						"but", "by", "for", "if", "in", "into", "is", "it",
						"no", "not", "of", "on", "or", "s", "such", "t",
						"that", "the", "their", "then", "there", "these",
						"they", "this", "to", "was", "will", "with" });

	
  public static void main(String[] args) {
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));
    
    // split each document into words
    JavaRDD<String> tokenized = sc.textFile(args[0]).flatMap(
      new FlatMapFunction<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
        public Iterable<String> call(String s) {
          return Arrays.asList(s.split(" "));
        }
      }
    );
    
 // filter out words with less than threshold occurrences
    JavaRDD<String> filtered = tokenized.filter(
      new Function<String, Boolean>() {
    
		private static final long serialVersionUID = 1L;

		@Override
        public Boolean call(String word) {
          return !STOP_WORDS.contains(word);
        }
      }
    );
    
    // count the occurrence of each word
    JavaPairRDD<String, Integer> counts = filtered.mapToPair(
      new PairFunction<String, String, Integer>() {
      
		private static final long serialVersionUID = 1L;

		@Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }
    ).reduceByKey(
      new Function2<Integer, Integer, Integer>() {
      
		private static final long serialVersionUID = 1L;

		@Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      }
    );
    
    counts.saveAsTextFile(args[1]);
    sc.close();
    
  }
}