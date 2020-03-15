package de.mklinger.blobstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.mklinger.blobstore.impl.AbstractBlobStoreWriter;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class RotatingFileBlobStoreWriter extends AbstractBlobStoreWriter {
	private static final Logger LOG = LoggerFactory.getLogger(RotatingFileBlobStoreWriter.class);

	private final File directory;
	private final String prefix;
	private final String suffix;
	private final FileBlobStoreWriter.Builder delegateBuilder;

	private final int maxEntryCount;
	private int entryCount;
	private BlobStoreWriter currentWriter;
	private final List<File> files;
	private int nextIdx;

	public RotatingFileBlobStoreWriter(Builder builder) {
		if (builder.maxEntryCountPerFile <= 0) {
			throw new IllegalArgumentException();
		}
		this.directory = Objects.requireNonNull(builder.directory);
		this.prefix = Objects.requireNonNull(builder.prefix);
		this.suffix = Objects.requireNonNull(builder.suffix);
		this.maxEntryCount = builder.maxEntryCountPerFile;
		this.delegateBuilder = FileBlobStoreWriter.builder()
				.withDefaults(builder.defaults)
				.withOverwrite(builder.overwrite)
				.withMaxIndexEntriesInMemory(builder.maxIndexEntriesInMemory);
		this.files = new ArrayList<>();
		this.nextIdx = 0;
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
			currentWriter = delegateBuilder
					.withBlobFile(f)
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

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private boolean overwrite;
		private File directory;
		private String prefix;
		private String suffix;
		private BlobStoreDefaults defaults = BlobStoreDefaults.STANDARD_DEFAULTS;
		private int maxIndexEntriesInMemory;
		private int maxEntryCountPerFile;

		public Builder withOverwrite(boolean overwrite) {
			this.overwrite = overwrite;
			return this;
		}

		public Builder withDirectory(File directory) {
			this.directory = directory;
			return this;
		}

		public Builder withPrefix(String prefix) {
			this.prefix = prefix;
			return this;
		}

		public Builder withSuffix(String suffix) {
			this.suffix = suffix;
			return this;
		}

		public Builder withDefaultMediaType(String mediaType) {
			this.defaults = new BlobStoreDefaults(mediaType, this.defaults.getDefaultEncoding());
			return this;
		}

		public Builder withDefaultEncoding(String encoding) {
			this.defaults = new BlobStoreDefaults(this.defaults.getDefaultMediaType(), encoding);
			return this;
		}

		public Builder withDefaults(BlobStoreDefaults defaults) {
			this.defaults = defaults;
			return this;
		}

		public Builder withMaxIndexEntriesInMemory(int maxIndexEntriesInMemory) {
			this.maxIndexEntriesInMemory = maxIndexEntriesInMemory;
			return this;
		}

		public Builder withMaxEntryCountPerFile(int maxEntryCountPerFile) {
			this.maxEntryCountPerFile = maxEntryCountPerFile;
			return this;
		}

		public RotatingFileBlobStoreWriter build() throws IOException {
			return new RotatingFileBlobStoreWriter(this);
		}
	}

}
