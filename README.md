# Cloudflow

Cloudflow is a MapReduce and Spark pipeline framework, which is based on a similar concept as JavaFlume or Apache Crunch. In contrast to these existing approaches, Cloudflow was developed to simplify the pipeline creation in biomedical research, especially in the field of Genetics. For that purpose Cloudflow supports a variety of NGS data formats and contains a rich collection of built-in operations for analyzing such kind of datasets (e.g. quality checks, mapping reads or variation calling).

The latest release is <b>0.6.0</b>, released December 21, 2015.



## Installation

Cloudflow is available in our Maven repository: 

Maven Repository:
```xml
<repository>
        <id>genepi-maven</id>
        <url>https://raw.github.com/genepi/maven-repository/mvn-repo/</url>
</repository>
```

Maven Dependency:
```xml
<dependencies>
	<dependency>
		<groupId>genepi</groupId>
		<artifactId>cloudflow</artifactId>
		<version>0.6.0</version>
	</dependency>
</dependencies>
```

A working example project can be found here: https://github.com/genepi/cloudflow-wordcount

## Getting Started

You can clone our example project to test cloudflow:

```shell
git clone https://github.com/genepi/cloudflow-wordcount
```

Next, you have to import the project into Eclipse or you can execute maven to build the jar file:

```shell
cd cloudflow-wordcount
mvn package
```

Maven creates the jar `target/cloudflow-wordcount-hadoop/cloudflow-0.6.0-wordcount.jar`, which includes all dependencies. The job can be execute with the following command:

### Running on MapReduce

```shell
hadoop jar, cloudflow-wordcount.jar mapreduce <hdfs_input> <hdfs_output>
```

### Running on Spark

```shell
/usr/bin/spark-submit --class genepi.cloudflow.examples.WordCount --master yarn cloudflow-wordcount.jar spark <hdfs_input> <hdfs_output>
```

or without YARN

```shell
/usr/bin/spark-submit --class genepi.cloudflow.examples.WordCount --master local cloudflow-wordcount.jar spark <local_input> <local_output>
```

More examples can be found here: https://github.com/seppinho/cloudflow/tree/master/cloudflow/src/cloudflow/examples

## Documentation

### Input Records

Cloudflow operates on records consisting of a key/value pair, whereby different record types are available (e.g. `TextRecord`, `IntegerRecord`, `FastqRecord`). A loader class (e.g. `TextLoader`, `FastqLoader`) is responsible to load the input data and to convert it into an appropriate record type.

### Transformer and Summarizer

Cloudflow supports three different basic operations, which can be used to analyze and transform records:

1. The `Transformer` is used to analyze one input record and to create 0 - n output records. The user implements the computational logic for this operation by extending an abstract class. This class provides a simple function, which is executed by our framework for all input records in parallel:
```java
class MyTransformer extends Tansformer<InRecord, OutRecord> {
    
    public MyTransformer(){
    	super(InRecord.class, OutRecord.class);
    }

    public void trasform(InRecord in) {
       doSomething();
       emit(new OutRecord(...));
    }
    
}
````

2. The `Summarizer` operates on a list of records, whereby records with the similar key are grouped. Thus, the signature of the process method has the key and a list of records as an input: 
```java
class MySummarizer extends Summarizer<InRecord, OutRecord>  {
    
   public MySummarizer(){
   	super(InRecord.class, OutRecord.class);
   }


   public void summarize(String key, List<InRecord> in) {
       doSomething();
       emit(new OutRecord(...));
    }
    
}
```

3. The GroupByKey operation is a special operation, which takes a list of records as an input and creates record group with the same key. Our framework inserts automatically a group-operation between a transform- and a summarize-operation. This ensures, that output records of the transform operation are compatible with the input records of the summarize operation.

### Pipelines

Pipelines are built by connecting several operations with compatible interfaces. For this purpose our framework implements the Builder pattern, which enables (a) building complex pipelines, (b) providing type safety and (c) the implementation of domains specific builders (see BioPipeline). Moreover, the Builder pattern ensures that only a valid sequence of operations can be created (i.e. after the group-by operation a summarize operation has to be added). 

This has the advantage that even a default WordCount example can be broken down into a few simple operations and is defined in a single line of code:

```java
class WordCount {

	public static void main(String[] args) throws IOException {
	
		String input = args[0];
		String output = args[1];
	
	        Pipeline pipeline = new Pipeline("WordCount", WordCount.class);
	        pipeline.loadText(input)
	                .apply(LineToWords.class)
	                .sum()
	                .save(output);
	        pipeline.run();
	        
    	}
    	
}
```

In a first step, the text file is loaded from HDFS (`loadText`). Then, for each record (i.e. line) we execute the application-specific `LineToWords` operation, which splits the line into words and creates for each word a new record. This operation is a extended `Transformer` class:

```java
class LineToWords extends Transformer {
   public void transform(TextRecord rec) {
     String[] words = rec.getValue().split()
     for (String word: words){
        emit(new IntegerRecord(word, 1));
     }
   }
}
```

In the last step we execute the predefined sum operation. It extends the pipeline by a group-by operation and a summarize operation in order to sum up all the values for a certain key (the complete example is available in `src/cloudflow/examples/WordCount.java`).


## BioPipeline

Cloudflow provides a variety of already implemented utilities which facilitate the creation of pipelines in the field of Bioinformatics (especially for NGS data in Genetics). For that purpose, we create the `BioPipeline` class, which extends the default `Pipeline` class by several domain specific features.

### Example: VCF Quality Check

A simple quality control pipeline for VCF files can be implemented by simple combining several built-in operations. First, we apply predefined filters to discard variations that are monomorphic, marked as duplicates or are Insertions or Deletions (InDels). For all records passing the filters, Cloudflow applies a summarize-operation that calculates the call rate for each variation. The Cloudflow pipeline has the following structure.

```java
class CallRateCalc extends Transformer {
  public void transform(VcfRecord record) {
    VariantContext snp = record.getValue();
    float call = callRate(snp);
    emit(new FloatRecord(snp.getID(), call);
  }
}

class VcfQualityCheck {
	public static void main(String[] args) throws IOException {
		BioPipeline pipeline = new BioPipeline("VCF-QC", VcfQualityCheck.class);
		pipeline.loadVCF(input)
		        .filter(MonomorphicFilter.class)
		        .filter(DuplicateFilter.class)
		        .filter(InDelFilter.class)
		        .apply(CallRateCalc.class)
		        .save(output);
		pipeline.run();
	}
}
```

### Records

We implemented several record types and loader classes in order to process FASTQ, BAM and VCF files (based on HadoopBAM):

| Data Format | Operation           | Record            | Key         | Value  |
|-------------|---------------------|-------------------|-------------|---|
|  **FASTQ**    | loadFastq(filename) | `FastqRecord`     | `String`    | `SequencedFragment` (see org.seqdoop.hadoop_bam.SequencedFragment)  |
|  **BAM**      | loadBam(filename)   | `BamRecord`       | `Integer`   | `SAMRecord` (see htsjdk.samtools.SAMRecord)  |
| **VCF**      | loadVcf(filename)   | `VcfRecord`       | `Integer`   | `VariantContext` (see htsjdk.variant.variantcontext.VariantContext)  |

### Operations

Cloudflow provied several built-in operations and filters for the analysis of biological datasets:

| Data Format |        | Pipeline Operation              | Description                                                        |
|-------------|--------|---------------------------------|--------------------------------------------------------------------|
| **FASTQ**       | Split  | `split()`                         | Find pairs (for paired-end reads)                                  |
|             | Filter | `filter(LowQualityReads.class)`   | Filters reads by quality                                           |
|             |        | `filter(SequenceLength.class)`    | Filters reads by sequence length                                   |
|             | Other  | `findPairedReads()`               | Detects read pairs                                                 |
|             |        | `align(referenceSequence)`        | Aligns sequences against a reference (using jBWA for alignment)    |
| **BAM**         | Split  | `split()`                         | Creates fixed size chunks (e.g. 64 MB)                             |
|             |        | `split(5, BamChunk.MBASES)`       | Creates logical chunks (e.g. 5MBases)                              |
|             | Filter | `filter(UnmappedReads.class)`     | Filters unmapped reads                                             |
|             |        | `filter(LowQualityReads.class)`   | Filters reads by map.quality                                       |
|             | Other  | `findVariations()`                | Finds variations in aligned reads (using samtools)                 |
| **VCF**         | Split  | `split()`                         | Creates fixed size chunks (e.g. 64 MB)                             |
|             |        | `split(5, VcfChunk.MBASES)`       | Creates logical chunks (e.g. 5MBases)                              |
|             | Filter | `filter(MonomorphicFilter.class)` | Filters monomorphic site                                           |
|             |        | `filter(DuplicateFilter.class)`   | Filters duplicates                                                 |
|             |        | `filter(InDelFilter.class)`      | Filters inDels                                                     |
|             |        | `filter(CallRateFilter.class)`    | Filters by call rate                                               |
|             |        | `filter(MafFilter.class)`         | Filters by MAF                                                     |
|             | Other  | `checkAlleleFreq(reference)`      | Allele frequency check with external reference (e.g. 1000 genomes) |

## Contributors

- Lukas Forer
- Sebastian Schönherr
- Enis Afgan
- Hansi Weißensteiner
- Davor Davidović
