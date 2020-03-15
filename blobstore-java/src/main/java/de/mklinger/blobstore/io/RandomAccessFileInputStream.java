package de.mklinger.blobstore.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class RandomAccessFileInputStream extends InputStream {
	private final RandomAccessFile f;
	private long readCount = 0;
	private final long length;

	public RandomAccessFileInputStream(final RandomAccessFile f, final long offset, final long length) throws IOException {
		this.f = f;
		this.length = length;
		this.f.seek(offset);
	}

	@Override
	public int read() throws IOException {
		if (readCount >= length) {
			return -1;
		}
		readCount++;
		return f.read();
	}

	@Override
	public int read(final byte[] b, final int off, final int len) throws IOException {
		long left = length - readCount;
		if (left > Integer.MAX_VALUE) {
			left = Integer.MAX_VALUE;
		}
		if (left <= 0) {
			return -1;
		}
		if (left < len) {
			final int n = f.read(b, off, (int)left);
			readCount += n;
			return n;
		} else {
			final int n = f.read(b, off, len);
			readCount += n;
			return n;
		}
	}

	@Override
	public void close() throws IOException {
		f.close();
	}
}