package de.mklinger.blobstore;

import static de.mklinger.blobstore.BlobEntry.ENCODING_IDENTITY;
import static java.nio.charset.Charset.defaultCharset;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.mklinger.blobstore.impl.BlobEntryImpl;

public class FileBlobStoreWriterTest {
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testText() throws IOException {
		final byte[][] datas = new byte['Z' - 'A' + 1][];
		for (char c = 'A'; c <= 'Z'; c++) {
			final byte[] data = new byte[100];
			for (int i = 0; i < data.length; i++) {
				data[i] = (byte)c;
			}
			datas[c - 'A'] = data;
		}

		final File blobFile = tmp.newFile("blob2.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(blobFile)
				.overwrite(true)
				.build()) {
			for (int i = 0; i < datas.length; i++) {
				writer.addBlobEntry("Entry" + (char)('A' + (char)i), new ByteArrayInputStream(datas[i]), "text/plain", ENCODING_IDENTITY);
			}
		}

		System.out.println(blobFile);

		final BlobStoreReader reader = new FileBlobStoreReader(blobFile);
		final int i[] = new int[1];
		reader.visitBlobEntries(new BlobEntryVisitor() {
			@Override
			public void visit(final BlobEntry blobEntry, final InputStream contents) {
				assertEquals("Entry" + (char)('A' + (char)i[0]), blobEntry.getName());
				assertEquals(100, blobEntry.getLength());
				assertEquals("text/plain", blobEntry.getMediaType());
				assertEquals(ENCODING_IDENTITY, blobEntry.getEncoding());
				i[0]++;
			}
		});
	}

	@Test
	public void testChunks() throws IOException {
		final File blobFilePlain = tmp.newFile("blob0.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(blobFilePlain)
				.overwrite(true)
				.maxIndexEntriesInMemory(-1)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", ENCODING_IDENTITY);
			}
		}

		final File blobFile1 = tmp.newFile("blob1.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(blobFile1)
				.overwrite(true)
				.maxIndexEntriesInMemory(1000)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", ENCODING_IDENTITY);
			}
		}

		final File blobFile2 = tmp.newFile("blob2.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(blobFile2)
				.overwrite(true)
				.maxIndexEntriesInMemory(100)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", ENCODING_IDENTITY);
			}
		}

		final File blobFile3 = tmp.newFile("blob3.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(blobFile3)
				.overwrite(true)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
		}

		assertEquals(readFileToString(blobFilePlain, defaultCharset()), readFileToString(blobFile1, defaultCharset()));
		assertEquals(readFileToString(blobFilePlain, defaultCharset()), readFileToString(blobFile2, defaultCharset()));
		assertEquals(readFileToString(blobFilePlain, defaultCharset()), readFileToString(blobFile3, defaultCharset()));
		assertEquals(readFileToString(blobFile1, defaultCharset()), readFileToString(blobFile2, defaultCharset()));
		assertEquals(readFileToString(blobFile1, defaultCharset()), readFileToString(blobFile3, defaultCharset()));
		assertEquals(readFileToString(blobFile2, defaultCharset()), readFileToString(blobFile3, defaultCharset()));

		// assert idx chunk files are gone
		final Set<String> validFilenames = new HashSet<>(Arrays.asList("blob0.bin", "blob1.bin", "blob2.bin", "blob3.bin"));
		final File[] files = tmp.getRoot().listFiles();
		assertEquals(4, files.length);
		for (final File file : files) {
			final String filename = file.getName();
			Assert.assertTrue("File should not exists: " + filename, validFilenames.contains(filename));
		}
	}

	@Test
	public void testWriteEncodedReadDecoded() throws IOException {
		final File blobFile = tmp.newFile();

		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.blobFile(blobFile)
				.overwrite(true)
				.build()) {
			writer.addBlobEntryGzEncoded("entry 1 gzip", new ByteArrayInputStream("entry 1 gzip VALUE".getBytes(UTF_8)));
			writer.addBlobEntryUnencoded("entry 2 identity", new ByteArrayInputStream("entry 2 identity VALUE".getBytes(UTF_8)));
		};

		final FileBlobStoreReader reader = new FileBlobStoreReader(blobFile);
		assertEquals("entry 1 gzip VALUE", IOUtils.toString(reader.getBlobEntryContentsDecoded("entry 1 gzip"), UTF_8));
		assertEquals("entry 2 identity VALUE", IOUtils.toString(reader.getBlobEntryContentsDecoded("entry 2 identity"), UTF_8));
	}
}
