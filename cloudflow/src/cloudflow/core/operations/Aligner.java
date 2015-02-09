package cloudflow.core.operations;

import genepi.io.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;

import cloudflow.bio.fastq.SingleRead;
import cloudflow.core.PipelineConf;
import cloudflow.core.hadoop.GroupedRecords;
import cloudflow.core.records.ShortReadRecord;
import cloudflow.core.records.TextRecord;

import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;

public class Aligner extends ReduceOperation<ShortReadRecord, TextRecord> {

	private TextRecord outRecord = new TextRecord();
	SingleRead first = new SingleRead();
	SingleRead second = new SingleRead();
	BwaIndex index;
	BwaMem mem;
	List<ShortRead> L1;
	List<ShortRead> L2;
	Text out;
	String[] result;
	int countReads;
	int trimBasesStart;
	int trimBasesEnd;
	String jbwaLibLocation;
	String referencePath;

	@Override
	public void configure(PipelineConf conf) {
		conf.set("mapred.child.java.opts", "-Xmx1000G");
		jbwaLibLocation = conf.getArchive("jbwa.tar.gz");
		referencePath = conf.getArchive("reference.tar.gz");

		String refString = null;
		L1 = new ArrayList<ShortRead>();
		L2 = new ArrayList<ShortRead>();
		out = new Text();
		countReads = 0;
		System.out.println("path is " + jbwaLibLocation);

		String[] files = FileUtil.getFiles(jbwaLibLocation, "*.*");
		System.out.println(Arrays.toString(files));

		String jbwaLib = FileUtil.path(jbwaLibLocation, "native",
				"libbwajni.so");
		/** load JNI */
		System.load(jbwaLib);

		File reference = new File(referencePath);
		refString = findFileinReferenceArchive(reference, ".fasta");

		String ref = FileUtil.path(refString);

		/** load index, aligner */
		try {
			index = new BwaIndex(new File(ref));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mem = new BwaMem(index);
	}

	public Aligner() {
		super(ShortReadRecord.class, TextRecord.class);

	}

	@Override
	public void process(String key, GroupedRecords<ShortReadRecord> values) {
		SingleRead first = new SingleRead();
		SingleRead second = new SingleRead();

		while (values.hasNextRecord()) {
			SingleRead value = values.getRecord().getWritableValue();
			countReads++;

			if (value.getReadNumber() == 1) {

				first = new SingleRead();
				/** copy object, otherwise same reference */
				first.setReadLength(value.getReadLength());
				first.setName(value.getName());
				first.setBases(value.getBases());
				first.setQual(value.getQualities());
				first.setFilename(value.getFilename());
				L1.add(first);

			} else {

				second = new SingleRead();
				second.setReadLength(value.getReadLength());
				second.setName(value.getName());
				second.setBases(value.getBases());
				second.setQual(value.getQualities());
				second.setFilename(value.getFilename());
				L2.add(second);

			}

			if (countReads % 99010 == 0) {

				System.out.println("count is " + countReads);
				/** main JBWA JNI */
				try {
					result = mem.align(L1, L2);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				for (int i = 0; i < result.length; i++) {

					String read = result[i];
					String sample = ((SingleRead) L1.get(i / 2)).getFilename();

					/**
					 * hack to write a valid SAM since BWA MEM outputs tabs at
					 * the end, samtools can not handle this
					 */
					read = read.replaceAll("\\t+$", "");
					read = read.replaceAll("\\s+$", "");
					String tiles[] = read.split("\t+\n");

					for (String tile : tiles) {
						out.clear();
						out.set(tile);
						outRecord.setKey(sample);
						outRecord.setValue(out.toString());
						emit(outRecord);

					}

				}

				L1.clear();
				L2.clear();
			}
		}
	}

	public static String findFileinReferenceArchive(File reference,
			String suffix) {
		String refPath = null;
		System.out.println(reference);
		if (reference.isDirectory()) {
			File[] files = reference.listFiles();
			for (File i : files) {
				if (i.getName().endsWith(suffix)) {

					refPath = i.getAbsolutePath();
				}
			}
		}
		System.out.println("path " + refPath);
		return refPath;
	}

	public static void showFiles(File[] files) {
		for (File file : files) {
			if (file.isDirectory()) {
				System.out.println("Directory: " + file.getName());
				showFiles(file.listFiles()); // Calls same method again.
			} else {
				System.out.println("File: " + file.getName());
			}
		}
	}

}