package de.mklinger.blobstore.impl;

import java.io.IOException;
import java.io.InputStream;

import de.mklinger.blobstore.BlobEntry;
import de.mklinger.blobstore.BlobStoreWriter;
import de.mklinger.blobstore.io.GzipCompressingInputStream;

public abstract class AbstractBlobStoreWriter implements BlobStoreWriter {
	@Override
	public void addBlobEntryGzEncoded(String name, InputStream nonGzIn) throws IOException {
		addBlobEntry(name, new GzipCompressingInputStream(nonGzIn), null, BlobEntry.ENCODING_GZIP);
	}

	@Override
	public void addBlobEntryGzEncoded(String name, InputStream nonGzIn, String mediaType) throws IOException {
		addBlobEntry(name, new GzipCompressingInputStream(nonGzIn), mediaType, BlobEntry.ENCODING_GZIP);
	}

	@Override
	public void addBlobEntryUnencoded(String name, InputStream in) throws IOException {
		addBlobEntry(name, in, null, BlobEntry.ENCODING_IDENTITY);
	}

	@Override
	public void addBlobEntryUnencoded(String name, InputStream in, String mediaType) throws IOException {
		addBlobEntry(name, in, mediaType, BlobEntry.ENCODING_IDENTITY);
	}
}
