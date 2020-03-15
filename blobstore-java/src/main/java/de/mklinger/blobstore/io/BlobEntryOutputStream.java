package de.mklinger.blobstore.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.mklinger.blobstore.BlobStoreWriter;
import de.mklinger.micro.streamcopy.StreamCopy;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class BlobEntryOutputStream extends ProxyOutputStream {
	private static final Logger LOG = LoggerFactory.getLogger(BlobEntryOutputStream.class);

	private static final int MAX_SIZE_IN_MEMORY = 1024 * 1024;
	private File tmpFile;

	private final BlobStoreWriter bsw;
	private final String name;
	private final String mediaType;
	private final String encoding;

	public BlobEntryOutputStream(final BlobStoreWriter bsw, final String name, final String mediaType, final String encoding) {
		super(new ByteArrayOutputStream());
		this.bsw = bsw;
		this.name = name;
		this.mediaType = mediaType;
		this.encoding = encoding;
	}

	@Override
	protected void beforeWrite(final int n) throws IOException {
		if (tmpFile == null && ((ByteArrayOutputStream)out).size() + n > MAX_SIZE_IN_MEMORY) {
			switchToFile();
		}
	}

	private void switchToFile() throws IOException {
		LOG.info("Switching to file");
		tmpFile = Files.createTempFile("blob", ".bin").toFile();
		final FileOutputStream newOut = new FileOutputStream(tmpFile);
		boolean success = false;
		try {
			final ByteArrayInputStream in = new ByteArrayInputStream(((ByteArrayOutputStream)out).toByteArray());
			StreamCopy.copy(in, newOut);
			success = true;
		} finally {
			if (!success) {
				newOut.close();
				Files.delete(tmpFile.toPath());
			}
		}
		out = newOut;
	}

	@Override
	public void close() throws IOException {
		if (tmpFile == null) {
			bsw.addBlobEntry(name, new ByteArrayInputStream(((ByteArrayOutputStream)out).toByteArray()), mediaType, encoding);
		} else {
			try {
				out.close();
				try (FileInputStream fin = new FileInputStream(tmpFile)) {
					bsw.addBlobEntry(name, fin, mediaType, encoding);
				}
			} finally {
				tmpFile.delete();
			}
		}
		out = ClosedOutputStream.CLOSED_OUTPUT_STREAM;
	}
}
