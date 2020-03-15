package de.mklinger.blobstore;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public interface BlobStoreReader {
	BlobEntry getBlobEntry(String name) throws IOException;

	InputStream getBlobEntryContents(BlobEntry blobEntry) throws IOException;
	InputStream getBlobEntryContentsDecoded(BlobEntry blobEntry) throws IOException;

	InputStream getBlobEntryContents(String name) throws IOException;
	InputStream getBlobEntryContentsDecoded(String name) throws IOException;

	void visitBlobEntries(BlobEntryVisitor visitor) throws IOException;
}