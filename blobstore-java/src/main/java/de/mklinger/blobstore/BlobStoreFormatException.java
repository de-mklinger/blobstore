package de.mklinger.blobstore;

public class BlobStoreFormatException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public BlobStoreFormatException() {
		super();
	}

	public BlobStoreFormatException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public BlobStoreFormatException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public BlobStoreFormatException(final String message) {
		super(message);
	}

	public BlobStoreFormatException(final Throwable cause) {
		super(cause);
	}
}
