package de.mklinger.blobstore.impl;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

import de.mklinger.blobstore.BlobEntry;
import de.mklinger.blobstore.BlobStoreDefaults;

/**
 * Entry in a blob store.
 *
 * Blob entry names must be US-ASCII between 32 (' ') and 126 ('~') and not contain 61 ('=').
 *
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class BlobEntryImpl implements BlobEntry, Comparable<BlobEntryImpl> {
	public static final String BLOB_ENTRY_NAME_ENCODING = "US-ASCII";
	private final String name;
	private final long offset;
	private final long length;
	private String mediaType;
	private String encoding;
	private final BlobStoreDefaults defaults;

	/**
	 * Create a blob entry with media type and encoding from the given defaults.
	 * @param name The entry name
	 * @param offset Offset in bytes
	 * @param length Length in bytes
	 * @param defaults Default settings
	 */
	public BlobEntryImpl(final String name, final long offset, final long length, BlobStoreDefaults defaults) {
		this(name, offset, length, null, null, defaults);
	}

	/**
	 * Create a blob entry.
	 * @param name The entry name
	 * @param offset Offset in bytes
	 * @param length Length in bytes
	 * @param mediaType The media type
	 * @param encoding The encoding
	 */
	public BlobEntryImpl(final String name, final long offset, final long length, final String mediaType, final String encoding, BlobStoreDefaults defaults) {
		this.name = requireLegalName(name);
		this.offset = offset;
		this.length = length;
		this.defaults = defaults;
		if (mediaType == null || mediaType.isEmpty() || defaults.getDefaultMediaType().equals(mediaType)) {
			this.mediaType = null;
		} else {
			this.mediaType = mediaType;
		}
		BlobEntry.requireValidEncoding(encoding);
		if (encoding == null || encoding.isEmpty() || defaults.getDefaultEncoding().equals(encoding)) {
			this.encoding = null;
		} else {
			this.encoding = encoding;
		}
	}

	private static String requireLegalName(String name) {
		Objects.requireNonNull(name);
		name.chars().forEach(BlobEntryImpl::requireLegalNameCharacter);
		return name;
	}

	private static int requireLegalNameCharacter(int c) {
		if (c < 32 || c > 126 || c == 61) { // 61 = '='
			throw new IllegalArgumentException("Blob entry name contains illegal character: '" + c + "'");
		}
		return c;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public long getOffset() {
		return offset;
	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public String getEncoding() {
		if (encoding == null) {
			return defaults.getDefaultEncoding();
		}
		return encoding;
	}

	@Override
	public String getMediaType() {
		if (mediaType == null) {
			return defaults.getDefaultMediaType();
		}
		return mediaType;
	}

	@Override
	public int compareTo(final BlobEntryImpl o) {
		return name.compareTo(o.getName());
	}

	@Override
	public boolean equals(final Object obj) {
		return obj instanceof BlobEntryImpl
				&& name.equals(((BlobEntryImpl)obj).getName());
	}

	public void write(final Writer w) throws IOException {
		w.write(name);
		w.write("=");
		w.write(String.valueOf(offset));
		w.write(";");
		w.write(String.valueOf(length));
		w.write(";");
		if (encoding != null) {
			w.write(encoding);
		}
		w.write(";");
		// media type comes last, it may contain ';'
		if (mediaType != null) {
			w.write(mediaType);
		}
		w.write("\n");
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append("BlobEntry [name=");
		builder.append(name);
		builder.append(", offset=");
		builder.append(offset);
		builder.append(", length=");
		builder.append(length);
		builder.append(", mediaType=");
		builder.append(mediaType);
		builder.append(", encoding=");
		builder.append(encoding);
		builder.append("]");
		return builder.toString();
	}

	public static BlobEntry parseBlobEntry(final String line, BlobStoreDefaults defaults) {
		final String key = parseKey(line);
		if (key == null) {
			return null;
		}
		final int valuesStartIdx = key.length() + 1;

		final int idx1 = line.indexOf(';', valuesStartIdx);
		final int idx2 = line.indexOf(';', idx1 + 1);
		final int idx3 = line.indexOf(';', idx2 + 1);

		final String offset = line.substring(valuesStartIdx, idx1);
		final String length = line.substring(idx1 + 1, idx2);

		String encoding;
		if (idx2 + 1 < idx3) {
			encoding = line.substring(idx2 + 1, idx3);
		} else {
			encoding = null;
		}

		// media type may contain ';', consume all the rest:
		String mediaType;
		if (idx3 + 1 < line.length()) {
			mediaType = line.substring(idx3 + 1);
		} else {
			mediaType = null;
		}

		return new BlobEntryImpl(
				key,
				Long.parseLong(offset),
				Long.parseLong(length),
				mediaType,
				encoding,
				defaults);
	}

	public static String parseKey(final String line) {
		if (line == null) {
			return null;
		}
		final int idx = line.indexOf('='); // '=' is illegal in key
		if (idx == -1) {
			throw new IllegalArgumentException("No key in line '" + line + "'");
		}
		return line.substring(0, idx);
	}
}