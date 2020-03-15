package de.mklinger.blobstore.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

public class ClosedOutputStream {

	public static final OutputStream CLOSED_OUTPUT_STREAM = closedOutputStream();

	private static OutputStream closedOutputStream() {
		final OutputStream out = OutputStream.nullOutputStream();
		try {
			out.close();
		} catch (final IOException e) {
			// should never happen
			throw new UncheckedIOException(e);
		}
		return out;
	}

	private ClosedOutputStream() {}
}
