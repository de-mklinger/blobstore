package de.mklinger.blobstore.io;

import java.io.IOException;
import java.io.OutputStream;

public class NonClosingCountingOutputStream extends NonClosingOutputStream {
	private long count = 0;

	public NonClosingCountingOutputStream(final OutputStream delegate) {
		super(delegate);
	}

	@Override
	public void write(final int b) throws IOException {
		super.write(b);
		count++;
	}

	@Override
	public void write(final byte[] b) throws IOException {
		super.write(b);
		count += b.length;
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {
		super.write(b, off, len);
		count += len;
	}

	public long getByteCount() {
		return count;
	}
}