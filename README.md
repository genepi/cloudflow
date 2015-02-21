# Cloudflow

Cloudflow is a MapReduce pipeline framework, which is based on a similar concept as JavaFlume or Apache Crunch. In contrast to these existing approaches, Cloudflow was developed to simplify the pipeline creation in biomedical research, especially in the field of Genetics. For that purpose Cloudflow supports a variety of NGS data formats and contains a rich collection of built-in operations for analyzing such kind of datasets (e.g. quality checks, mapping reads or variation calling).

The latest release is <b>0.5.0</b>, released February 23, 2015.



## Installation

Cloudflow is available in our Maven repository: 

Maven Repository:
```
TODO
```

Maven Dependency:
```
TODO
```
## Getting Started

TODO: simple wordcount pipeline

```java
Class LineToWords extends Transformer {
   public void transform(TextRecord rec) {
     String[] words = rec.getValue().split()
     for (String word: words){
        emit(new IntegerRecord(word, 1));
     }
   }
}

pipeline.loadText(input)
        .transform(LineToWords.class)
        .sum()
        .save(output);
```

## Examples

all examples from paper

```java
Class CallRateCalc extends Transformer {
  public void transform(VcfRecord record) {
    VariantContext snp = record.getValue();
    float call = callRate(snp);
    emit(new FloatRecord(snp.getID(), call);
  }
}

pipeline.loadVCF(input)
        .filter(MonomorphicFilter.class)
        .filter(DuplicateFilter.class)
        .filter(InDelFilter.class)
        .transform(CallRateCalc.class)
        .save(output);
```



## Supported Operations

Cloudflow provides a variety of already implemented utilities which facilitate the creation of pipelines in the field of Bioinformatics (especially for NGS data in Genetics). For that purpose, we implemented based on HadoopBAM several record types and loader classes in order to process FASTQ, BAM and VCF files. Moreover, we created several operations and filters for the analysis of biological datasets.

### FASTQ

Unaligned reads in FASTQ format.

##### Split
```
split()
```
Find pairs (for paired-end reads)

##### Filter
```
filter(LowQualityReads.class)
```
Filters reads by quality

#####	Other
```
align(referenceSequence)
```
Aligns sequences against a reference (using jBWA for alignment)


### BAM

Align reads in BAM format.

##### Split
```
split()
```
Creates fixed size chunks (e.g. 64 MB)

```
split(5, BamChunk.MBASES)
```
Creates logical chunks (e.g. 5MBases)

##### Filter
```
filter(UnmappedReads.class)
```
Filters unmapped reads

```
filter(LowQualityReads.class)
```
Filters reads by map.quality

##### Other
```
findVariations()
```

Finds variations in aligned reads (using samtools)

### VCF

Variations stored in VCF files.

##### Split

```
split()
```
Creates fixed size chunks (e.g. 64 MB)

```
split(5, VcfChunk.MBASES)
```
Creates logical chunks (e.g. 5MBases)

##### Filter

```
filter(MonomorphicFilter.class)
```
Filters monomorphic site

```
filter(DuplicateFilter.class)
```
Filters duplicates

```
filter(InDelFilter.class)
```
Filters inDels
	
```
filter(CallRateFilter.class)
```
Filters by call rate

```
filter(MafFilter.class)
```
Filters by MAF

#####	Other

```
checkAlleleFreq(reference)
```
Allele frequency check with external reference (e.g. 1000 genomes)
