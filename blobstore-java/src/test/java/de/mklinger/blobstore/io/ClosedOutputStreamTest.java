package de.mklinger.blobstore.io;

import static de.mklinger.blobstore.io.ClosedOutputStream.CLOSED_OUTPUT_STREAM;

import java.io.IOException;

import org.junit.Test;

public class ClosedOutputStreamTest {
	@Test(expected = IOException.class)
	public void testWrite1() throws IOException {
		CLOSED_OUTPUT_STREAM.write((byte) 0);
	}

	@Test(expected = IOException.class)
	public void testWrite2() throws IOException {
		CLOSED_OUTPUT_STREAM.write(new byte[] { 1, 2, 3 });
	}

	@Test(expected = IOException.class)
	public void testWrite3() throws IOException {
		CLOSED_OUTPUT_STREAM.write(new byte[] { 1, 2, 3 }, 1, 1);
	}

	@Test
	public void testCloseAgain() throws IOException {
		CLOSED_OUTPUT_STREAM.close();
	}
}
