package de.mklinger.blobstore.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Input stream that provides gzip compressed data.
 * @author Marc Klinger - mklinger[at]mklinger[dot]de
 */
public class GzipCompressingInputStream extends InputStream {
	private final List<byte[]> buffers;
	private int bufferOffset;
	private boolean writeDone;

	private final InputStream in;
	private final ByteArrayOutputStream bout;
	private final GZIPOutputStream gzout;
	private byte[] inOutBuf;

	public GzipCompressingInputStream(final InputStream in) throws IOException {
		if (in == null) {
			throw new NullPointerException();
		}
		this.in = in;
		this.bout = new ByteArrayOutputStream();
		this.gzout = new GZIPOutputStream(bout);
		this.inOutBuf = new byte[4096];
		this.buffers = new LinkedList<>();
	}

	private synchronized void setWriteDone() throws IOException {
		gzout.close();
		inOutBuf = null;
		writeDone = true;
	}

	private synchronized void setReadDone() throws IOException {
		setWriteDone();
		buffers.clear();
		in.close();
	}

	@Override
	public synchronized int read() throws IOException {
		while (buffers.isEmpty()) {
			if (writeDone) {
				// buffer is empty and we won't get new data -> EOF
				return -1;
			}
			fillBuffers();
		}
		final byte[] buffer = buffers.get(0);
		final int availableLength = buffer.length - bufferOffset;
		if (availableLength == 1) {
			final byte b = buffer[bufferOffset];
			buffers.remove(0);
			bufferOffset = 0;
			return b;
		} else {
			final byte b = buffer[bufferOffset];
			bufferOffset++;
			return b;
		}
	}

	@Override
	public synchronized int read(final byte[] target, final int offset, final int len) throws IOException {
		if (target == null) {
			throw new NullPointerException();
		} else if (offset < 0 || len < 0 || len > target.length - offset) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return 0;
		}
		while (buffers.isEmpty()) {
			if (writeDone) {
				// buffer is empty and we won't get new data -> EOF
				return -1;
			}
			fillBuffers();
		}
		final byte[] buffer = buffers.get(0);
		final int availableLength = buffer.length - bufferOffset;
		if (len >= availableLength) {
			// write all and remove buffer from list
			System.arraycopy(buffer, bufferOffset, target, offset, availableLength);
			buffers.remove(0);
			bufferOffset = 0;
			return availableLength;
		} else {
			// write len and set bufferOffset
			System.arraycopy(buffer, bufferOffset, target, offset, len);
			bufferOffset += len;
			return len;
		}
	}

	private void fillBuffers() throws IOException {
		// synchronized by read method
		final int read = in.read(inOutBuf);
		if (read == -1) {
			setWriteDone();
		} else {
			gzout.write(inOutBuf, 0, read);
		}
		if (bout.size() > 0) {
			final byte[] outBytes = bout.toByteArray();
			bout.reset();
			buffers.add(outBytes);
		}
	}

	@Override
	public void close() throws IOException {
		setReadDone();
	}
}