package de.mklinger.blobstore;

import java.util.Objects;

public class BlobStoreDefaults {
	public static BlobStoreDefaults STANDARD_DEFAULTS = new BlobStoreDefaults("application/octet-stream", BlobEntry.ENCODING_GZIP);

	private final String defaultMediaType;
	private final String defaultEncoding;

	public BlobStoreDefaults(String defaultMediaType, String defaultEncoding) {
		this.defaultMediaType = Objects.requireNonNull(defaultMediaType);
		this.defaultEncoding = BlobEntry.requireValidEncoding(defaultEncoding);
	}

	public String getDefaultMediaType() {
		return defaultMediaType;
	}

	public String getDefaultEncoding() {
		return defaultEncoding;
	}
}
