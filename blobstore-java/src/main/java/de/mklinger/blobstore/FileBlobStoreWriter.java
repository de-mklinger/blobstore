package de.mklinger.blobstore;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.mklinger.blobstore.impl.AbstractBlobStoreWriter;
import de.mklinger.blobstore.impl.BlobEntryImpl;
import de.mklinger.blobstore.io.BlobEntryOutputStream;
import de.mklinger.blobstore.io.ExternalSort;
import de.mklinger.blobstore.io.NonClosingCountingOutputStream;
import de.mklinger.micro.streamcopy.StreamCopy;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class FileBlobStoreWriter extends AbstractBlobStoreWriter {
	private static final Logger LOG = LoggerFactory.getLogger(FileBlobStoreWriter.class);

	protected static final String HEADER_PREFIX = "indexOffset=";
	protected static final String HEADER_SUFFIX = "\n";
	protected static final String EMPTY_HEADER = HEADER_PREFIX + "___________________" + HEADER_SUFFIX;
	protected static final long HEADER_LENGTH = 32;
	private final int maxIndexEntriesInMemory;
	private final File blobFile;
	/** null if index is to be written to blob file. */
	private final File indexFile;
	private final BlobStoreDefaults defaults;
	private List<BlobEntryImpl> indexEntries;
	private final List<File> indexChunkFiles;
	private final Object indexEntriesMutex;
	private final NonClosingCountingOutputStream countingOut;

	private FileBlobStoreWriter(Builder builder) throws IOException {
		this.blobFile = Objects.requireNonNull(builder.blobFile);
		this.indexFile = builder.indexFile;
		this.maxIndexEntriesInMemory = builder.maxIndexEntriesInMemory;
		this.defaults = Objects.requireNonNull(builder.defaults);

		if (indexFile != null && blobFile.getAbsoluteFile().equals(indexFile.getAbsoluteFile())) {
			throw new IllegalArgumentException();
		}

		deleteIfOverwrite(builder.blobFile, builder.overwrite);
		deleteIfOverwrite(builder.indexFile, builder.overwrite);

		this.indexEntries = new ArrayList<>();
		this.indexChunkFiles = new ArrayList<>();
		this.indexEntriesMutex = new Object();
		this.countingOut = new NonClosingCountingOutputStream(new BufferedOutputStream(new FileOutputStream(blobFile)));

		if (indexFile == null) {
			// reserve header space
			this.countingOut.write(EMPTY_HEADER.getBytes(BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING));
		}
	}

	private void deleteIfOverwrite(File file, boolean overwrite) throws IOException {
		if (file != null && file.exists()) {
			if (!overwrite) {
				throw new IOException("File exists and overwrite is disabled: " + file.getAbsolutePath());
			}
			Files.delete(file.toPath());
		}
	}

	@Override
	public void addBlobEntry(final String name, final InputStream in, final String mediaType, final String encoding) throws IOException {
		final long offset;
		final long length;
		synchronized (countingOut) {
			offset = countingOut.getByteCount();
			StreamCopy.copy(in, countingOut);
			length = countingOut.getByteCount() - offset;
		}
		addIndexEntry(new BlobEntryImpl(name, offset, length, mediaType, encoding, defaults));
	}

	/**
	 * Get an output stream suitable for writing a new blob entry.
	 * The returned output stream is not thread-safe.
	 */
	public OutputStream getBlobEntryOutputStream(final String name, final String mediaType, final String encoding) {
		return new BlobEntryOutputStream(this, name, mediaType, encoding);
	}

	@Override
	public void mergeFrom(final BlobStoreReader reader) throws IOException {
		reader.visitBlobEntries(new BlobEntryVisitor() {
			@Override
			public void visit(final BlobEntry blobEntry, final InputStream contents) throws IOException {
				final long offset;
				final long length;
				synchronized (countingOut) {
					offset = countingOut.getByteCount();
					StreamCopy.copy(contents, countingOut);
					length = countingOut.getByteCount() - offset;
				}
				addIndexEntry(new BlobEntryImpl(blobEntry.getName(), offset, length, blobEntry.getMediaType(), blobEntry.getEncoding(), defaults));
			}
		});
	}

	private void addIndexEntry(final BlobEntryImpl indexEntry) throws IOException {
		List<BlobEntryImpl> indexEntriesToDump = null;
		synchronized (indexEntriesMutex) {
			indexEntries.add(indexEntry);
			if (maxIndexEntriesInMemory > 0 && indexEntries.size() >= maxIndexEntriesInMemory) {
				indexEntriesToDump = indexEntries;
				indexEntries = new ArrayList<>();
			}
		}
		dumpIndexEntriesChunk(indexEntriesToDump);
	}

	private void dumpIndexEntriesChunk(final List<BlobEntryImpl> indexEntries) throws IOException, UnsupportedEncodingException, FileNotFoundException {
		if (indexEntries != null) {
			LOG.info("Dumping {} index entries to chunk file...", indexEntries.size());
			// sort current index entries and dump to temp file
			Collections.sort(indexEntries);
			final File indexChunkFile = File.createTempFile(blobFile.getName(), ".idxchunk", blobFile.getParentFile());
			synchronized (indexChunkFiles) {
				indexChunkFiles.add(indexChunkFile);
			}
			try (Writer idxOut = newIndexChunkWriter(indexChunkFile)) {
				for (final BlobEntryImpl entry : indexEntries) {
					entry.write(idxOut);
				}
			}
		}
	}

	private OutputStreamWriter newIndexChunkWriter(final File indexChunkFile) throws IOException {
		return new OutputStreamWriter(
				new GZIPOutputStream(
						new BufferedOutputStream(
								new FileOutputStream(indexChunkFile))),
				BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING);
	}

	@Override
	public void close() throws IOException {
		final long indexOffset;
		synchronized (countingOut) {
			indexOffset = countingOut.getByteCount();

			try {
				if (indexFile == null) {
					writeIndex(countingOut);
				} else {
					try (FileOutputStream indexOut = new FileOutputStream(indexFile)) {
						writeIndex(indexOut);
					}
				}
			} finally {
				countingOut.reallyClose();
			}
		}

		if (indexFile == null) {
			writeHeader(indexOffset);
		}
	}

	private void writeIndex(OutputStream indexOut) throws IOException {
		if (!indexChunkFiles.isEmpty()) {
			// dump remaining entries
			synchronized (indexEntriesMutex) {
				dumpIndexEntriesChunk(indexEntries);
			}
			indexEntries = null;

			// do external sort on chunk files
			try (BufferedWriter idxOut = new BufferedWriter(new OutputStreamWriter(indexOut, BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING))) {
				ExternalSort.mergeSortedFiles(
						indexChunkFiles,
						idxOut,
						BLOB_ENTRY_LINE_COMPARATOR,
						Charset.forName(BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING),
						false,
						true);
			}
		} else {
			// sort in-memory and write to blob file index section
			synchronized (indexEntriesMutex) {
				Collections.sort(indexEntries);
				try (BufferedWriter idxOut = new BufferedWriter(new OutputStreamWriter(indexOut, BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING))) {
					for (final BlobEntryImpl entry : indexEntries) {
						entry.write(idxOut);
					}
				}
				indexEntries = null;
			}
		}
	}

	private static final Comparator<String> BLOB_ENTRY_LINE_COMPARATOR = new Comparator<String>() {
		@Override
		public int compare(final String o1, final String o2) {
			final String k1 = BlobEntryImpl.parseKey(o1);
			final String k2 = BlobEntryImpl.parseKey(o2);
			return k1.compareTo(k2);
		}
	};

	private void writeHeader(final long indexOffset) throws IOException {
		final StringBuilder paddedIndexOffsetSb = new StringBuilder(19); // Long.MAX_VALUE is 9223372036854775807 and thus maximum 19 decimal digits
		final String indexOffsetString = String.valueOf(indexOffset);
		final int length = indexOffsetString.length();
		if (length > 19) {
			// should never happen for a positive long
			throw new IllegalStateException();
		}
		for (int i = length; i < 19; i++) {
			paddedIndexOffsetSb.append('0');
		}
		paddedIndexOffsetSb.append(indexOffsetString);
		final String paddedIndexOffset = paddedIndexOffsetSb.toString();

		try (RandomAccessFile randomAccessFile = new RandomAccessFile(blobFile, "rw")) {
			randomAccessFile.write(HEADER_PREFIX.getBytes(BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING));
			randomAccessFile.write(paddedIndexOffset.getBytes(BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING));
			randomAccessFile.write(HEADER_SUFFIX.getBytes(BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING));
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		// package protected
		static final int DEFAULT_MAX_ENTRIES_IN_MEMORY = 100_000;

		private boolean overwrite;
		private File blobFile;
		private File indexFile;
		private BlobStoreDefaults defaults = BlobStoreDefaults.STANDARD_DEFAULTS;
		private int maxIndexEntriesInMemory = DEFAULT_MAX_ENTRIES_IN_MEMORY;

		public Builder overwrite(boolean overwrite) {
			this.overwrite = overwrite;
			return this;
		}

		public Builder blobFile(File blobFile) {
			this.blobFile = blobFile;
			return this;
		}

		/**
		 * Enable usage of a index file and do not write index data into the blob file.
		 * Do not set, or set to <code>null</code>, to produce a combined blob file.
		 */
		public Builder indexFile(File indexFile) {
			this.indexFile = indexFile;
			return this;
		}

		public Builder defaultMediaType(String mediaType) {
			this.defaults = new BlobStoreDefaults(mediaType, this.defaults.getDefaultEncoding());
			return this;
		}

		public Builder defaultEncoding(String encoding) {
			this.defaults = new BlobStoreDefaults(this.defaults.getDefaultMediaType(), encoding);
			return this;
		}

		public Builder defaults(BlobStoreDefaults defaults) {
			this.defaults = defaults;
			return this;
		}

		/**
		 * Set number of index entries to keep in memory before dumping a index chunk to
		 * disk. A value &lt;= 0 disables dumping of index chunks to disk, all index
		 * entries will be kept in memory.
		 *
		 * <p>
		 * Default value: {@value #DEFAULT_MAX_ENTRIES_IN_MEMORY}.
		 * </p>
		 */
		public Builder maxIndexEntriesInMemory(int maxIndexEntriesInMemory) {
			this.maxIndexEntriesInMemory = maxIndexEntriesInMemory;
			return this;
		}

		public FileBlobStoreWriter build() throws IOException {
			return new FileBlobStoreWriter(this);
		}
	}
}
