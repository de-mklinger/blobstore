package de.mklinger.blobstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiFileBlobStoreReader implements BlobStoreReader {
	private static final Logger LOG = LoggerFactory.getLogger(MultiFileBlobStoreReader.class);

	private final List<BlobStoreReader> readers;

	public MultiFileBlobStoreReader(final List<File> files) throws IOException {
		readers = new ArrayList<>(files.size());
		for (final File file : files) {
			try {
				readers.add(new FileBlobStoreReader(file));
			} catch (final BlobStoreFormatException e) {
				LOG.warn("Not using blob file {} as input, seems to be broken", file);
			}
		}
	}

	public List<BlobStoreReader> getReaders() {
		return Collections.unmodifiableList(readers);
	}

	@Override
	public BlobEntry getBlobEntry(final String name) throws IOException {
		for (final BlobStoreReader reader : readers) {
			final BlobEntry blobEntry = reader.getBlobEntry(name);
			if (blobEntry != null) {
				return new ReaderBoundBlobEntry(blobEntry, reader);
			}
		}
		return null;
	}

	@Override
	public InputStream getBlobEntryContentsDecoded(BlobEntry blobEntry) throws IOException {
		final ReaderBoundBlobEntry e = requireReaderBoundBlobEntry(blobEntry);
		return e.getReader().getBlobEntryContentsDecoded(e.getDelegate());
	}

	@Override
	public InputStream getBlobEntryContents(final BlobEntry blobEntry) throws IOException {
		final ReaderBoundBlobEntry e = requireReaderBoundBlobEntry(blobEntry);
		return e.getReader().getBlobEntryContents(e.getDelegate());
	}

	private ReaderBoundBlobEntry requireReaderBoundBlobEntry(BlobEntry blobEntry) {
		Objects.requireNonNull(blobEntry);
		if (!(blobEntry instanceof ReaderBoundBlobEntry)) {
			throw new IllegalArgumentException();
		}
		return (ReaderBoundBlobEntry) blobEntry;
	}

	@Override
	public InputStream getBlobEntryContentsDecoded(String name) throws IOException {
		final BlobEntry entry = getBlobEntry(name);
		if (entry == null) {
			return null;
		}
		return getBlobEntryContentsDecoded(entry);
	}

	@Override
	public InputStream getBlobEntryContents(final String name) throws IOException {
		final BlobEntry entry = getBlobEntry(name);
		if (entry == null) {
			return null;
		}
		return getBlobEntryContents(entry);
	}

	@Override
	public void visitBlobEntries(final BlobEntryVisitor visitor) throws IOException {
		for (final BlobStoreReader reader : readers) {
			reader.visitBlobEntries((entry, contents)
					-> visitor.visit(new ReaderBoundBlobEntry(entry, reader), contents));
		}
	}

	private static class ReaderBoundBlobEntry implements BlobEntry {
		private final BlobEntry delegate;
		private final BlobStoreReader reader;

		public ReaderBoundBlobEntry(final BlobEntry delegate, final BlobStoreReader reader) {
			this.delegate = delegate;
			this.reader = reader;
		}

		public BlobStoreReader getReader() {
			return reader;
		}

		public BlobEntry getDelegate() {
			return delegate;
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public long getOffset() {
			return delegate.getOffset();
		}

		@Override
		public long getLength() {
			return delegate.getLength();
		}

		@Override
		public String getEncoding() {
			return delegate.getEncoding();
		}

		@Override
		public String getMediaType() {
			return delegate.getMediaType();
		}
	}
}
