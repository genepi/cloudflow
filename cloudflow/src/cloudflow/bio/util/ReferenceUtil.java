package cloudflow.bio.util;

import htsjdk.samtools.SAMRecord.SAMTagAndValue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class ReferenceUtil {

	
	
	public static String findFileinReferenceArchive(File reference, String suffix) {
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
	
	public static boolean checkValidChromosomeM(String chromosome) {
		return chromosome.equalsIgnoreCase("CHRM") || chromosome.equalsIgnoreCase("MT") || chromosome.equalsIgnoreCase("RCRS") ||  chromosome.equalsIgnoreCase("RSRS") ||chromosome.equalsIgnoreCase("gi|251831106|ref|NC_012920.1|") || chromosome.equalsIgnoreCase("gi|17981852|ref|NC_001807.4|");
	}
	
	// for BAQ calculation to get a correct reference
	public static String getValidReferenceName (String reference){
		String alteredRef;
		
		switch(reference.toUpperCase())
		{
			case "CHRM": case "MT" : case "RCRS":
				//alteredRef = "gi|251831106|ref|NC_012920.1|";
				alteredRef = "rCRS";
			   break; 
			default:
				alteredRef = reference;
		}
		
		return alteredRef;
	}

	public static int getTagFromSamRecord(List<SAMTagAndValue> attList, String att) {
		int value = 30;
		for (SAMTagAndValue member : attList) {
			if (member.tag.equals(att))
				value = (int) member.value;
		}
		return value;
	}
	
	
	
	public static String readInReference(String file)  {
		StringBuilder stringBuilder = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = null;
			stringBuilder = new StringBuilder();
			
			while ((line = reader.readLine()) != null) {

				if (!line.startsWith(">"))
					stringBuilder.append(line);

			}
			
			reader.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	

		return stringBuilder.toString();
	}
	
	public static String getSelectedReferenceArchive(String reference) {
		String archive;
		switch(reference)
		{
			case "rcrs": 
				archive = "rcrs.tar.gz";
			   break; 
			case "rsrs": 
				archive = "rsrs.tar.gz";
			   break; 
			case "hg19": 
				archive = "hg19.tar.gz";
			   break; 
			default:
				archive = "rcrs.tar.gz";
		}
		return archive;
	}
	
	public static String getFastaPath(String reference) {
		String archive;
		
		switch(reference)
		{
			case "rcrs": 
				archive = "rCRS.fasta";
			   break; 
			case "rsrs": 
				archive = "rsrs.fasta";
			   break; 
			case "hg19": 
				archive = "hg19.fasta";
			   break; 
			default:
				archive = "rCRS.fasta";
		}
		return archive;
	}
	
	
}
