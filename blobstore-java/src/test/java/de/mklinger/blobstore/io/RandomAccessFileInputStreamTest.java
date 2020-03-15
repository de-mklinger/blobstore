package de.mklinger.blobstore.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.mklinger.blobstore.io.RandomAccessFileInputStream;

public class RandomAccessFileInputStreamTest {
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testOffset() throws IOException {
		final File f = tmp.newFile();
		FileUtils.writeByteArrayToFile(f, new byte[] { 1, 2, 3, 4, 5, 6 });
		try (InputStream in = new RandomAccessFileInputStream(new RandomAccessFile(f, "r"), 0, f.length())) {
			assertEquals(1, in.read());

			final byte[] buf = new byte[2];

			buf[0] = 0;
			buf[1] = 0;
			int n = in.read(buf);
			assertEquals(2, n);
			assertArrayEquals(new byte[] { 2, 3 }, buf);

			buf[0] = 0;
			buf[1] = 0;
			n = in.read(buf, 0, 2);
			assertEquals(2, n);
			assertArrayEquals(new byte[] { 4, 5 }, buf);

			buf[0] = 0;
			buf[1] = 0;
			n = in.read(buf, 1, 2);
			assertEquals(1, n);
			assertArrayEquals(new byte[] { 0, 6 }, buf);

			n = in.read(buf);
			assertEquals(-1, n);
		}
	}
}
