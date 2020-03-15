package de.mklinger.blobstore.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public class LazyInputStream extends InputStream {
	private final InputStreamSupplier factory;
	private volatile InputStream delegate;

	@FunctionalInterface
	public static interface InputStreamSupplier {
		InputStream get() throws IOException;
	}

	public LazyInputStream(InputStreamSupplier factory) {
		this.factory = factory;
	}

	private InputStream getDelegate() throws IOException {
		InputStream tmp = delegate;

		if (tmp == null) {
			synchronized (this) {
				tmp = delegate;
				if (tmp == null) {
					delegate = tmp = factory.get();
				}
			}
		}

		return tmp;
	}

	@Override
	public int read() throws IOException {
		return getDelegate().read();
	}

	@Override
	public int read(byte[] b) throws IOException {
		return getDelegate().read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return getDelegate().read(b, off, len);
	}

	@Override
	public long skip(long n) throws IOException {
		return getDelegate().skip(n);
	}

	@Override
	public int available() throws IOException {
		return getDelegate().available();
	}

	@Override
	public void close() throws IOException {
		if (delegate != null) {
			getDelegate().close();
		}
	}

	@Override
	public void mark(int readlimit) {
		try {
			getDelegate().mark(readlimit);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void reset() throws IOException {
		getDelegate().reset();
	}

	@Override
	public boolean markSupported() {
		try {
			return getDelegate().markSupported();
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
