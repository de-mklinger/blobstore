package de.mklinger.blobstore;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public interface BlobEntryVisitor {
	void visit(BlobEntry blobEntry, InputStream contents) throws IOException;
}
