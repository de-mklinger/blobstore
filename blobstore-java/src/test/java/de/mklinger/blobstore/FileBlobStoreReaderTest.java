package de.mklinger.blobstore;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileBlobStoreReaderTest {
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		final Map<String, String> testData = new HashMap<>();

		final int testDataCount = 1000;

		for (int i = 0; i < testDataCount; i++) {
			final String name = getRandomString(getRandomInt(6)) + i + getRandomString(getRandomInt(6));
			final String data = getRandomString(getRandomInt(500));
			testData.put(name, data);
		}

		final File f = tmp.newFile();
		try (FileBlobStoreWriter w = FileBlobStoreWriter.builder()
				.withBlobFile(f)
				.withOverwrite(true)
				.build()) {
			for (final Entry<String, String> e : testData.entrySet()) {
				try (ByteArrayInputStream in = new ByteArrayInputStream(e.getValue().getBytes(StandardCharsets.UTF_8))) {
					w.addBlobEntry(e.getKey(), in, "application/something", BlobEntry.ENCODING_IDENTITY);
				}
			}
		}

		read(testData, f, 0);
		read(testData, f, 20);
	}

	private void read(final Map<String, String> testData, final File f, final int positionCacheDepth) throws IOException {
		final int readRunCount = 3;
		for (int i = 0; i < readRunCount; i++) {

			final long start = System.currentTimeMillis();

			final FileBlobStoreReader r = new FileBlobStoreReader(f, positionCacheDepth);

			for (final Entry<String, String> e : testData.entrySet()) {
				final BlobEntry blobEntry = r.getBlobEntry(e.getKey());
				assertEquals("application/something", blobEntry.getMediaType());
				assertEquals(BlobEntry.ENCODING_IDENTITY, blobEntry.getEncoding());
				String actualData;
				try (InputStream in = r.getBlobEntryContents(blobEntry)) {
					actualData = IOUtils.toString(in, StandardCharsets.UTF_8);
				}
				assertEquals(e.getValue(), actualData);
			}

			final long end = System.currentTimeMillis();

			System.out.println("Time (run " + i + "@" + positionCacheDepth + "): " + (end - start) + "ms");
		}
	}

	private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	private static final SecureRandom RANDOM = new SecureRandom();

	private int getRandomInt(final int max) {
		return RANDOM.nextInt(max - 1) + 1;
	}

	private String getRandomString(final int len) {
		final char[] c = new char[len];
		for (int i = 0; i < c.length; i++) {
			c[i] = ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length()));
		}
		return new String(c);
	}
}
