package de.mklinger.blobstore;

import static de.mklinger.blobstore.Random.getRandomInt;
import static de.mklinger.blobstore.Random.getRandomString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.mklinger.micro.streamcopy.StreamCopy;

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
				try (ByteArrayInputStream in = new ByteArrayInputStream(e.getValue().getBytes(UTF_8))) {
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
					actualData = IOUtils.toString(in, UTF_8);
				}
				assertEquals(e.getValue(), actualData);
			}

			final long end = System.currentTimeMillis();

			System.out.println("Time (run " + i + "@" + positionCacheDepth + "): " + (end - start) + "ms");
		}
	}

	@Test
	public void testReadV1() throws IOException {
		final File blobFile = tmp.newFile();

		try (InputStream in = getClass().getResourceAsStream("v1.blob")) {
			try (FileOutputStream fout = new FileOutputStream(blobFile)) {
				StreamCopy.copy(in, fout);
			}
		}

		final FileBlobStoreReader reader = new FileBlobStoreReader(blobFile);
		assertEquals("entry 1 gzip VALUE", IOUtils.toString(reader.getBlobEntryContentsDecoded("entry 1 gzip"), UTF_8));
		assertEquals("entry 2 identity VALUE", IOUtils.toString(reader.getBlobEntryContentsDecoded("entry 2 identity"), UTF_8));
	}
}
