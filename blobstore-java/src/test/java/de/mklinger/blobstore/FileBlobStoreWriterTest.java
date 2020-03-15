package de.mklinger.blobstore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
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
				.withBlobFile(blobFile)
				.withOverwrite(true)
				.build()) {
			for (int i = 0; i < datas.length; i++) {
				writer.addBlobEntry("Entry" + (char)('A' + (char)i), new ByteArrayInputStream(datas[i]), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
		}

		System.out.println(blobFile);

		final BlobStoreReader reader = new FileBlobStoreReader(blobFile);
		final int i[] = new int[1];
		reader.visitBlobEntries(new BlobEntryVisitor() {
			@Override
			public void visit(final BlobEntry blobEntry, final InputStream contents) {
				Assert.assertEquals("Entry" + (char)('A' + (char)i[0]), blobEntry.getName());
				Assert.assertEquals(100, blobEntry.getLength());
				Assert.assertEquals("text/plain", blobEntry.getMediaType());
				Assert.assertEquals(BlobEntryImpl.ENCODING_IDENTITY, blobEntry.getEncoding());
				i[0]++;
			}
		});
	}

	@Test
	public void testChunks() throws IOException {
		final File blobFilePlain = tmp.newFile("blob0.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.withBlobFile(blobFilePlain)
				.withOverwrite(true)
				.withMaxIndexEntriesInMemory(-1)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
		}

		final File blobFile1 = tmp.newFile("blob1.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.withBlobFile(blobFile1)
				.withOverwrite(true)
				.withMaxIndexEntriesInMemory(1000)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
		}

		final File blobFile2 = tmp.newFile("blob2.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.withBlobFile(blobFile2)
				.withOverwrite(true)
				.withMaxIndexEntriesInMemory(100)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
		}

		final File blobFile3 = tmp.newFile("blob3.bin");
		try (BlobStoreWriter writer = FileBlobStoreWriter.builder()
				.withBlobFile(blobFile3)
				.withOverwrite(true)
				.build()) {
			for (int i = 9876; i >= 0; i--) {
				writer.addBlobEntry("Entry" + i, new ByteArrayInputStream(("data" + i).getBytes()), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
		}

		Assert.assertEquals(FileUtils.readFileToString(blobFilePlain, Charset.defaultCharset()), FileUtils.readFileToString(blobFile1, Charset.defaultCharset()));
		Assert.assertEquals(FileUtils.readFileToString(blobFilePlain, Charset.defaultCharset()), FileUtils.readFileToString(blobFile2, Charset.defaultCharset()));
		Assert.assertEquals(FileUtils.readFileToString(blobFilePlain, Charset.defaultCharset()), FileUtils.readFileToString(blobFile3, Charset.defaultCharset()));
		Assert.assertEquals(FileUtils.readFileToString(blobFile1, Charset.defaultCharset()), FileUtils.readFileToString(blobFile2, Charset.defaultCharset()));
		Assert.assertEquals(FileUtils.readFileToString(blobFile1, Charset.defaultCharset()), FileUtils.readFileToString(blobFile3, Charset.defaultCharset()));
		Assert.assertEquals(FileUtils.readFileToString(blobFile2, Charset.defaultCharset()), FileUtils.readFileToString(blobFile3, Charset.defaultCharset()));

		// assert idx chunk files are gone
		final Set<String> validFilenames = new HashSet<>(Arrays.asList("blob0.bin", "blob1.bin", "blob2.bin", "blob3.bin"));
		final File[] files = tmp.getRoot().listFiles();
		Assert.assertEquals(4, files.length);
		for (final File file : files) {
			final String filename = file.getName();
			Assert.assertTrue("File should not exists: " + filename, validFilenames.contains(filename));
		}
	}
}
