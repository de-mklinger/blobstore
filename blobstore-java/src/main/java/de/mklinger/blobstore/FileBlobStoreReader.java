package de.mklinger.blobstore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

import de.mklinger.blobstore.io.LazyInputStream;
import de.mklinger.blobstore.io.RandomAccessFileInputStream;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class FileBlobStoreReader implements BlobStoreReader {
	private final File blobFile;
	private final BlobStoreDefaults defaults;
	private final long fileSize;
	private final long indexOffset;
	private final int positionCacheDepth;

	private Map<Long, String> linesCache;

	public FileBlobStoreReader(final File blobFile) throws IOException {
		this(blobFile, BlobStoreDefaults.STANDARD_DEFAULTS);
	}

	public FileBlobStoreReader(final File blobFile, BlobStoreDefaults defaults) throws IOException {
		this(blobFile, 20, defaults);
	}

	/**
	 * @param positionCacheDepth A value less than 1 disables caching.
	 */
	protected FileBlobStoreReader(final File blobFile, final int positionCacheDepth) throws IOException {
		this(blobFile, positionCacheDepth, BlobStoreDefaults.STANDARD_DEFAULTS);
	}

	/**
	 * @param positionCacheDepth A value less than 1 disables caching.
	 */
	protected FileBlobStoreReader(final File blobFile, final int positionCacheDepth, BlobStoreDefaults defaults) throws IOException {
		this.blobFile = blobFile;
		this.fileSize = blobFile.length();
		this.indexOffset = readIndexOffset();
		this.positionCacheDepth = positionCacheDepth;
		if (positionCacheDepth > 0) {
			linesCache = new ConcurrentHashMap<>();
		}
		this.defaults = defaults;
	}

	public File getBlobFile() {
		return blobFile;
	}

	private long readIndexOffset() throws IOException {
		String header;
		try (BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(blobFile), BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING))) {
			header = r.readLine();
		}
		if (header == null || header.length() != FileBlobStoreWriter.HEADER_LENGTH - 1 || !header.startsWith(FileBlobStoreWriter.HEADER_PREFIX)) {
			throw new BlobStoreFormatException("Invalid header in blob file: " + this.blobFile);
		}
		final String indexOffsetString = header.substring(FileBlobStoreWriter.HEADER_PREFIX.length());
		try {
			return Long.parseLong(indexOffsetString);
		} catch (final NumberFormatException e) {
			throw new BlobStoreFormatException("Invalid header in blob file: " + this.blobFile, e);
		}
	}

	@Override
	public BlobEntry getBlobEntry(final String name) throws IOException {
		try (final RandomAccessFile f = new RandomAccessFile(blobFile, "r")) {
			return searchBlobEntry(f, name);
		}
	}

	@Override
	public InputStream getBlobEntryContentsDecoded(BlobEntry blobEntry) throws IOException {
		if (BlobEntry.ENCODING_GZIP.equals(blobEntry.getEncoding())) {
			return new GZIPInputStream(getBlobEntryContents(blobEntry));
		} else {
			return getBlobEntryContents(blobEntry);
		}
	}

	@Override
	public InputStream getBlobEntryContents(final BlobEntry blobEntry) throws IOException {
		if (blobEntry == null) {
			throw new NullPointerException();
		}
		final RandomAccessFile f = new RandomAccessFile(blobFile, "r");
		return new RandomAccessFileInputStream(f, blobEntry.getOffset(), blobEntry.getLength());
	}

	@Override
	public InputStream getBlobEntryContentsDecoded(String name) throws IOException {
		final RandomAccessFile f = new RandomAccessFile(blobFile, "r");
		final BlobEntry blobEntry = searchBlobEntry(f, name);
		if (blobEntry == null) {
			f.close();
			return null;
		}

		final RandomAccessFileInputStream baseIn = new RandomAccessFileInputStream(f, blobEntry.getOffset(), blobEntry.getLength());
		if (BlobEntry.ENCODING_GZIP.equals(blobEntry.getEncoding())) {
			return new GZIPInputStream(baseIn);
		} else {
			return baseIn;
		}
	}

	@Override
	public InputStream getBlobEntryContents(final String name) throws IOException {
		final RandomAccessFile f = new RandomAccessFile(blobFile, "r");
		final BlobEntry blobEntry = searchBlobEntry(f, name);
		if (blobEntry == null) {
			f.close();
			return null;
		}
		return new RandomAccessFileInputStream(f, blobEntry.getOffset(), blobEntry.getLength());
	}

	@Override
	public void visitBlobEntries(final BlobEntryVisitor visitor) throws IOException {
		try (BufferedReader r = newIndexReader()) {
			String line;
			while ((line = r.readLine()) != null) {
				final BlobEntry blobEntry = BlobEntryImpl.parseBlobEntry(line, defaults);
				try (InputStream in = new LazyInputStream(() -> getBlobEntryContents(blobEntry.getName()))) {
					visitor.visit(blobEntry, in);
				}
			}
		}
	}

	private BufferedReader newIndexReader() throws IOException {
		@SuppressWarnings("resource")
		final FileInputStream fin = new FileInputStream(blobFile);
		try {
			long skipped = 0;
			while (skipped < indexOffset) {
				final long skippedNow = fin.skip(indexOffset - skipped);
				if (skippedNow < 0) {
					throw new IOException("Could not skip to index offset");
				}
				skipped += skippedNow;
			}
			return new BufferedReader(new InputStreamReader(fin, BlobEntryImpl.BLOB_ENTRY_NAME_ENCODING));
		} catch (final Exception e) {
			try {
				fin.close();
			} catch (final Exception e2) {
				if (e2 != e) {
					e.addSuppressed(e2);
				}
			}
			throw e;
		}
	}

	private BlobEntry searchBlobEntry(final RandomAccessFile indexRAFile, final String searchName) throws IOException {
		// because we read the second line after each seek there is no way the
		// binary search will find the first line, so check it first.
		String line = readFirstLine(indexRAFile);
		String key = BlobEntryImpl.parseKey(line);
		if (key == null) {
			return null;
		}
		if (key.equals(searchName)) {
			// the start is greater than or equal to the target, so it is what
			// we are looking for.
			return BlobEntryImpl.parseBlobEntry(line, defaults);
		}

		// set up the binary search.
		int iterationIdx = 0;
		long beg = indexOffset;
		long end = fileSize;
		while (beg <= end) {
			// find the mid point.
			final long mid = beg + (end - beg) / 2;
			line = readNextLine(indexRAFile, mid, iterationIdx);
			if (line == null) {
				// end of file, look before
				end = mid - 1;
			} else {
				key = BlobEntryImpl.parseKey(line);
				final int n = key.compareTo(searchName);
				if (n > 0) {
					// what we found is greater than the target, so look
					// before it.
					end = mid - 1;
				} else if (n < 0) {
					// otherwise, look after it.
					beg = mid + 1;
				} else {
					return BlobEntryImpl.parseBlobEntry(line, defaults);
				}
			}
			iterationIdx++;
		}

		// The search falls through when the range is narrowed to nothing.
		line = readNextLine(indexRAFile, beg, iterationIdx);
		final BlobEntry blobEntry = BlobEntryImpl.parseBlobEntry(line, defaults);
		if (blobEntry != null && !blobEntry.getName().equals(searchName)) {
			// not found
			return null;
		}
		return blobEntry;
	}

	private String readFirstLine(final RandomAccessFile indexRAFile) throws IOException {
		if (positionCacheDepth > 0) {
			final String line = linesCache.get(-1L);
			if (line != null) {
				return line;
			}
		}
		indexRAFile.seek(indexOffset);
		final String line = indexRAFile.readLine();
		if (line != null && positionCacheDepth > 0) {
			linesCache.putIfAbsent(-1L, line);
		}
		return line;
	}

	private String readNextLine(final RandomAccessFile indexRAFile, final long pos, final int iterationIdx) throws IOException {
		if (iterationIdx < positionCacheDepth) {
			final String line = linesCache.get(pos);
			if (line != null) {
				return line;
			}
		}
		indexRAFile.seek(pos);
		indexRAFile.readLine();
		final String line = indexRAFile.readLine();
		if (line != null && iterationIdx < positionCacheDepth) {
			linesCache.put(pos, line);
		}
		return line;
	}
}
