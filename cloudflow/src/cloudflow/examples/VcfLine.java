package cloudflow.examples;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class VcfLine {
    private String ref;
    private List<String> alts;
    private BigInteger pos;
    private String chrom;
    private boolean isIndel;

    public VcfLine(String line) {
        StringTokenizer tokenizer = new StringTokenizer(line);
        List<String> vcfFields = new ArrayList<String>();
        while (tokenizer.hasMoreTokens()) {
            vcfFields.add(tokenizer.nextToken());
        }
        setChrom(vcfFields.get(0));
        setPos(new BigInteger(vcfFields.get(1)));
        setRef(vcfFields.get(3));
        alts = new ArrayList<String>(Arrays.asList(vcfFields.get(4).split(",")));
        isIndel = line.contains("INDEL");
    }

    public List<String> getTiTv() {
        List<String> answer = new ArrayList<String>();
        if (isIndel()) {
            return answer;
        }
        for (String alt : getAlts()) {
            answer.add(getRef()+" -> "+alt);
        }
        return answer;
    }

    private boolean isIndel() {
        return isIndel;
    }

    public String getRef() {
        return ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public List<String> getAlts() {
        return alts;
    }

    public void setAlts(List<String> alts) {
        this.alts = alts;
    }

    public BigInteger getPos() {
        return pos;
    }

    public void setPos(BigInteger pos) {
        this.pos = pos;
    }

    public String getChrom() {
        return chrom;
    }

    public void setChrom(String chrom) {
        this.chrom = chrom;
    }
}
