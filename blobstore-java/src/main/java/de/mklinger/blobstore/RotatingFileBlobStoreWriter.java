package de.mklinger.blobstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.mklinger.blobstore.io.GzipCompressingInputStream;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class RotatingFileBlobStoreWriter implements BlobStoreWriter {
	private static final Logger LOG = LoggerFactory.getLogger(RotatingFileBlobStoreWriter.class);

	private final File directory;
	private final String prefix;
	private final String suffix;

	private final int maxEntryCount;
	private final BlobStoreDefaults defaults;
	private int entryCount;
	private BlobStoreWriter currentWriter;
	private final List<File> files;
	private int nextIdx;

	public RotatingFileBlobStoreWriter(final File directory, final String prefix, final String suffix, final int maxEntryCount) {
		this(directory, prefix, suffix, maxEntryCount, BlobStoreDefaults.STANDARD_DEFAULTS);
	}

	public RotatingFileBlobStoreWriter(final File directory, final String prefix, final String suffix, final int maxEntryCount, BlobStoreDefaults defaults) {
		if (maxEntryCount <= 0) {
			throw new IllegalArgumentException();
		}
		this.directory = Objects.requireNonNull(directory);
		this.prefix = Objects.requireNonNull(prefix);
		this.suffix = Objects.requireNonNull(suffix);
		this.maxEntryCount = maxEntryCount;
		this.defaults = defaults;
		this.files = new ArrayList<>();
		this.nextIdx = 0;
	}

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

	@Override
	public synchronized void addBlobEntry(final String name, final InputStream in, final String mediaType, final String encoding) throws IOException {
		if (currentWriter != null && entryCount >= maxEntryCount) {
			LOG.info("Rotating blob file...");
			currentWriter.close();
			currentWriter = null;
			entryCount = 0;
			LOG.info("Rotating blob file done.");
		}
		if (currentWriter == null) {
			final File f = getNextFile();
			if (f.exists()) {
				throw new IOException("Target file already exists: " + f.getAbsolutePath());
			}
			currentWriter = FileBlobStoreWriter.builder()
					.withBlobFile(f)
					.withOverwrite(true)
					.withDefaults(defaults)
					.build();
		}
		entryCount++;
		currentWriter.addBlobEntry(name, in, mediaType, encoding);
	}

	private synchronized File getNextFile() {
		File f;
		do {
			final String idx = String.format("%03d", nextIdx);
			nextIdx++;
			f = new File(directory, prefix + idx + suffix);
		} while (f.exists());
		files.add(f);
		return f;
	}

	public List<File> getFiles() {
		return new ArrayList<>(files);
	}

	@Override
	public void mergeFrom(final BlobStoreReader reader) throws IOException {
		reader.visitBlobEntries((blobEntry, contents)
				-> addBlobEntry(blobEntry.getName(), contents, blobEntry.getMediaType(), blobEntry.getEncoding()));
	}

	@Override
	public synchronized void close() throws IOException {
		if (currentWriter != null) {
			final BlobStoreWriter w = currentWriter;
			currentWriter = null;
			w.close();
		}
	}
}
