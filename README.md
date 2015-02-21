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
        .apply(CallRateCalc.class)
        .save(output);
```



## Supported Operations

Cloudflow provides a variety of already implemented utilities which facilitate the creation of pipelines in the field of Bioinformatics (especially for NGS data in Genetics). For that purpose, we implemented several record types and loader classes in order to process FASTQ, BAM and VCF files (based on HadoopBAM). Moreover, we created several operations and filters for the analysis of biological datasets.

### FASTQ

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

### BAM

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

### VCF

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
