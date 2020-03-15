package de.mklinger.blobstore.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import de.mklinger.blobstore.io.GzipCompressingInputStream;

/**
 * @author Marc Klinger - mklinger[at]mklinger[dot]de - klingerm
 */
public class GzipCompressingInputStreamTest {
	@Test
	public void testIOUtilsCopy() throws IOException {
		testWithCopyStrategy(new CopyStrategy() {
			@Override
			public void copy(final InputStream in, final OutputStream out) throws IOException {
				IOUtils.copy(in, out);
			}
		});
	}

	@Test
	public void testSmallChunkCopy() throws IOException {
		testWithCopyStrategy(new CopyStrategy() {
			@Override
			public void copy(final InputStream in, final OutputStream out) throws IOException {
				final byte[] buffer = new byte[10];
				int n = 0;
				while (-1 != (n = in.read(buffer))) {
					out.write(buffer, 0, n);
				}
			}
		});
	}

	@Test
	public void test1ByteChunkCopy() throws IOException {
		testWithCopyStrategy(new CopyStrategy() {
			@Override
			public void copy(final InputStream in, final OutputStream out) throws IOException {
				final byte[] buffer = new byte[1];
				int n = 0;
				while (-1 != (n = in.read(buffer))) {
					out.write(buffer, 0, n);
				}
			}
		});
	}

	@Test
	public void test1ByteReadCopy() throws IOException {
		testWithCopyStrategy(new CopyStrategy() {
			@Override
			public void copy(final InputStream in, final OutputStream out) throws IOException {
				int n = 0;
				while (-1 != (n = in.read())) {
					out.write(n);
				}
			}
		});
	}

	@Test
	public void testMixedCopy() throws IOException {
		testWithCopyStrategy(new CopyStrategy() {
			@Override
			public void copy(final InputStream in, final OutputStream out) throws IOException {
				while (true) {
					int n = 0;
					n = in.read();
					if (n == -1) {
						return;
					}
					out.write(n);

					final byte[] buffer1 = new byte[1];
					n = in.read(buffer1);
					if (n == -1) {
						return;
					}
					out.write(buffer1, 0, n);

					final byte[] buffer10 = new byte[10];
					n = in.read(buffer10);
					if (n == -1) {
						return;
					}
					out.write(buffer10, 0, n);

					n = in.read(buffer10, 5, 2);
					if (n == -1) {
						return;
					}
					out.write(buffer10, 5, n);
				}
			}
		});
	}

	private void testWithCopyStrategy(final CopyStrategy copyStrategy) throws IOException {
		final byte[] testData = getTestData();
		final ByteArrayOutputStream tmp = new ByteArrayOutputStream();
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(testData))) {
			copyStrategy.copy(in, tmp);
		}

		final ByteArrayOutputStream result = new ByteArrayOutputStream();
		try (GZIPInputStream gzin = new GZIPInputStream(new ByteArrayInputStream(tmp.toByteArray()))) {
			IOUtils.copy(gzin, result);
		}

		Assert.assertArrayEquals(testData, result.toByteArray());
	}

	private interface CopyStrategy {
		void copy(InputStream in, OutputStream out) throws IOException;
	}

	byte[] getTestData() {
		final int numSameBytes = 1024;
		final byte[] data = new byte[(Math.abs(Byte.MIN_VALUE) + Byte.MAX_VALUE) * numSameBytes];
		int idx = 0;
		for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
			for (int i = 0; i < numSameBytes; i++) {
				data[idx] = b;
				idx++;
			}
		}
		return data;
	}

	@Test(expected = NullPointerException.class)
	public void testCreateNull() throws IOException {
		new GzipCompressingInputStream(null).close();
	}

	@Test(expected = NullPointerException.class)
	public void testReadNull() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			in.read(null);
		}
	}

	@Test(expected = NullPointerException.class)
	public void testReadNull2() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			in.read(null, 0, 100);
		}
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testReadIOOB() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			in.read(new byte[100], -1, 100);
		}
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testReadIOOB2() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			in.read(new byte[100], 0, -1);
		}
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testReadIOOB3() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			in.read(new byte[100], 100, 1);
		}
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testReadIOOB4() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			in.read(new byte[100], 0, 101);
		}
	}

	@Test
	public void testReadZero() throws IOException {
		try (GzipCompressingInputStream in = new GzipCompressingInputStream(new ByteArrayInputStream(new byte[0]))) {
			final int count = in.read(new byte[100], 50, 0);
			Assert.assertEquals(0, count);
		}
	}
}
