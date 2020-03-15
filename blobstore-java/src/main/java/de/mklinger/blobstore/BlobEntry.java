package de.mklinger.blobstore;

public interface BlobEntry {
	String ENCODING_IDENTITY = "identity";
	String ENCODING_GZIP = "gzip";

	String getName();
	long getOffset();
	long getLength();
	String getEncoding();
	String getMediaType();

	public static String requireValidEncoding(String encoding) {
		if (encoding != null && !BlobEntry.ENCODING_GZIP.equals(encoding) && !BlobEntry.ENCODING_IDENTITY.equals(encoding)) {
			throw new IllegalArgumentException("Unsupported encoding: " + encoding);
		}
		return encoding;
	}
}