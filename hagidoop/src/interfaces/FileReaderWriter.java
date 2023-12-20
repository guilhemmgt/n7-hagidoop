package interfaces;

import io.AccessMode;

public interface FileReaderWriter extends ReaderWriter {
	public static final int FMT_TXT = 0;
	public static final int FMT_KV = 1;
	public void open(AccessMode mode);
	public void close();
	public long getIndex();
	public String getFname();
	public void setFname(String fname);
	public long getFsize();	// rajouté par nous
}
