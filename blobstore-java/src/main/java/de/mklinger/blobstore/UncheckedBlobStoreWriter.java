package de.mklinger.blobstore;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public class UncheckedBlobStoreWriter implements BlobStoreWriter {
	private final BlobStoreWriter delegate;

	public UncheckedBlobStoreWriter(BlobStoreWriter delegate) {
		this.delegate = delegate;
	}

	@Override
	public void addBlobEntry(String name, InputStream in, String mediaType, String encoding) {
		try {
			delegate.addBlobEntry(name, in, mediaType, encoding);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void addBlobEntryGzEncoded(String name, InputStream nonGzIn, String mediaType) {
		try {
			delegate.addBlobEntryGzEncoded(name, nonGzIn, mediaType);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void addBlobEntryGzEncoded(String name, InputStream nonGzIn) {
		try {
			delegate.addBlobEntryGzEncoded(name, nonGzIn);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void addBlobEntryUnencoded(String name, InputStream in, String mediaType) {
		try {
			delegate.addBlobEntryUnencoded(name, in, mediaType);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void addBlobEntryUnencoded(String name, InputStream in) {
		try {
			delegate.addBlobEntryUnencoded(name, in);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void close() {
		try {
			delegate.close();
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@Override
	public void mergeFrom(BlobStoreReader reader) {
		try {
			delegate.mergeFrom(reader);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
