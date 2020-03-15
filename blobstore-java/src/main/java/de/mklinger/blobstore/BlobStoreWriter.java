package de.mklinger.blobstore;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public interface BlobStoreWriter extends Closeable {
	void addBlobEntry(String name, InputStream in, String mediaType, String encoding) throws IOException;

	void addBlobEntryGzEncoded(String name, InputStream nonGzIn, String mediaType) throws IOException;
	void addBlobEntryGzEncoded(String name, InputStream nonGzIn) throws IOException;

	void addBlobEntryUnencoded(String name, InputStream in, String mediaType) throws IOException;
	void addBlobEntryUnencoded(String name, InputStream in) throws IOException;

	void mergeFrom(BlobStoreReader reader) throws IOException;
}