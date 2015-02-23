# Cloudflow

Cloudflow is a MapReduce pipeline framework, which is based on a similar concept as JavaFlume or Apache Crunch. In contrast to these existing approaches, Cloudflow was developed to simplify the pipeline creation in biomedical research, especially in the field of Genetics. For that purpose Cloudflow supports a variety of NGS data formats and contains a rich collection of built-in operations for analyzing such kind of datasets (e.g. quality checks, mapping reads or variation calling).

The latest release is <b>0.5.0</b>, released February 23, 2015.



## Installation

Cloudflow is available in our Maven repository: 

Maven Repository:
```
<repository>
	<id>cloudflow</id>
	<url>https://raw.github.com/genepi/maven/cloudflow</url>
	<snapshots>
		<enabled>true</enabled>
		<updatePolicy>always</updatePolicy>
	</snapshots>
</repository>
```

Maven Dependency:
```
<dependency>
	<groupId>genepi</groupId>
	<artifactId>cloudflow</artifactId>
	<version>0.5.0</version>
</dependency>

```
## Getting Started

### Input Records

Cloudflow operates on records consisting of a key/value pair, whereby different record types are available (e.g. `TextRecord`, `IntegerRecord`, `FastqRecord`). A loader class (e.g. `TextLoader`, `FastqLoader`) is responsible to load the input data and to convert it into an appropriate record type.

### Transformer and Summarize

Cloudflow supports three different basic operations, which can be used to analyze and transform records:

1. The `Transformer` is used to analyze one input record and to create 0 - n output records. The user implements the computational logic for this operation by extending an abstract class. This class provides a simple function, which is executed by our framework for all input records in parallel:
```java
class MyTransformer extends Tansformer<InRecord, OutRecord> {
    public void trasform(InRecord in) {
       doSomething();
       emit(new OutRecord(...));
    }
}
````

2. The `Summarizer` operates on a list of records, whereby records with the similar key are grouped. Thus, the signature of the process method has the key and a list of records as an input: 
```java
class MySummarizer extends Summarizer<InRecord, OutRecord>  {
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
	        Pipeline pipeline = new Pipeline(""WordCount", );
	        pipeline.loadText(input)
	                .transform(LineToWords.class)
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

### Example: Vcf Quality Check

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

##### FASTQ

- class: `FastqRecord`
- key: `String`
- value: `SequencedFragment` (see org.seqdoop.hadoop_bam.SequencedFragment)

##### BAM

- class: `BamRecord`
- key: `Integer`
- value: `SAMRecord` (see htsjdk.samtools.SAMRecord)

##### VCF

- class: `VcfRecord`
- key: `Integer`
- value: `VariantContext` (see htsjdk.variant.variantcontext.VariantContext)

### Operations

Cloudflow provied several built-in operations and filters for the analysis of biological datasets:

##### FASTQ

Find pairs (for paired-end reads):
```
split()
```

Filters reads by quality:
```
filter(LowQualityReads.class)
```

Aligns sequences against a reference (using jBWA for alignment)
```
align(referenceSequence)
```

##### BAM

Creates fixed size chunks (e.g. 64 MB):
```
split()
```

Creates logical chunks (e.g. 5MBases):
```
split(5, BamChunk.MBASES)
```

Filters unmapped reads:
```
filter(UnmappedReads.class)
```

Filters reads by map.quality:
```
filter(LowQualityReads.class)
```

Finds variations in aligned reads (using samtools):
```
findVariations()
```

##### VCF

Creates fixed size chunks (e.g. 64 MB):
```
split()
```

Creates logical chunks (e.g. 5MBases):
```
split(5, VcfChunk.MBASES)
```

Filters monomorphic site:
```
filter(MonomorphicFilter.class)
```

Filters duplicates:
```
filter(DuplicateFilter.class)
```

Filters inDels:
```
filter(InDelFilter.class)
```

Filters by call rate:
```
filter(CallRateFilter.class)
```

Filters by MAF:
```
filter(MafFilter.class)
```

Allele frequency check with external reference (e.g. 1000 genomes):
```
checkAlleleFreq(reference)
```
