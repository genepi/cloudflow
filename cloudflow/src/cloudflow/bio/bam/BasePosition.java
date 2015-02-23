package cloudflow.bio.bam;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BasePosition implements Writable {

	private int a = 0;

	private int c = 0;

	private int g = 0;

	private int n = 0;

	private int t = 0;

	public void add(BasePosition postion) {

		a += postion.a;
		c += postion.c;
		g += postion.g;
		t += postion.t;
		n += postion.n;

	}

	public void addA(int aFor) {
		this.a += aFor;
	}

	public void addC(int cFor) {
		this.c += cFor;
	}

	public void addG(int gFor) {
		this.g += gFor;
	}

	public void addN(int nFor) {
		this.n += nFor;
	}

	public void addT(int tFor) {
		this.t += tFor;
	}

	public void clear() {

		a = 0;
		c = 0;
		g = 0;
		t = 0;
		n = 0;

	}

	public int getA() {
		return a;
	}

	public int getC() {
		return c;
	}

	public int getG() {
		return g;
	}

	public int getN() {
		return n;
	}

	public int getT() {
		return t;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {

		a = arg0.readInt();
		c = arg0.readInt();
		g = arg0.readInt();
		t = arg0.readInt();
		n = arg0.readInt();

	}

	public void setA(int aFor) {
		this.a = aFor;
	}

	public void setC(int cFor) {
		this.c = cFor;
	}

	public void setG(int gFor) {
		this.g = gFor;
	}

	public void setN(int nFor) {
		this.n = nFor;
	}

	public void setT(int tFor) {
		this.t = tFor;
	}

	public char getTopBase() {

		if (getA() >= getC() && getA() >= getG() && getA() >= getT()) {
			return 'A';
		}

		else if (getC() >= getA() && getC() >= getG() && getC() >= getT()) {
			return 'C';
		}

		else if (getG() >= getC() && getG() >= getA() && getG() >= getT()) {
			return 'G';
		}

		else if (getT() >= getC() && getT() >= getG() && getT() >= getA()) {
			return 'T';
		}

		return 'N';
	}

	@Override
	public String toString() {

		return a + "\t" + c + "\t" + g + "\t" + t + "\t" + n;
	}

	@Override
	public void write(DataOutput arg0) throws IOException {

		arg0.writeInt(a);
		arg0.writeInt(c);
		arg0.writeInt(g);
		arg0.writeInt(t);
		arg0.writeInt(n);

	}

}
