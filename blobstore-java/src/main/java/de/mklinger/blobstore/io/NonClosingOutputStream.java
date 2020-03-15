package de.mklinger.blobstore.io;

import java.io.IOException;
import java.io.OutputStream;

public class NonClosingOutputStream extends OutputStream {
	private final OutputStream delegate;

	public NonClosingOutputStream(final OutputStream delegate) {
		this.delegate = delegate;
	}

	@Override
	public void write(final int b) throws IOException {
		delegate.write(b);
	}

	@Override
	public void write(final byte[] b) throws IOException {
		delegate.write(b);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {
		delegate.write(b, off, len);
	}

	@Override
	public void flush() throws IOException {
		delegate.flush();
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

	public void reallyClose() throws IOException {
		delegate.close();
	}
}