package de.mklinger.blobstore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.mklinger.blobstore.impl.BlobEntryImpl;

public class RotatingFileBlobStoreWriterTest {
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void test() throws IOException {

		final byte[][] datas = new byte['Z' - 'A' + 1][];
		for (char c = 'A'; c <= 'Z'; c++) {
			final byte[] data = new byte[100];
			for (int i = 0; i < data.length; i++) {
				data[i] = (byte)c;
			}
			datas[c - 'A'] = data;
		}

		List<File> files;
		final File directory = tmp.newFolder();

		try (RotatingFileBlobStoreWriter writer = RotatingFileBlobStoreWriter.builder()
				.withDirectory(directory)
				.withPrefix("blob")
				.withSuffix(".bin")
				.withMaxEntryCountPerFile(1)
				.build()) {

			for (int i = 0; i < datas.length; i++) {
				writer.addBlobEntry("Entry" + (char)('A' + (char)i), new ByteArrayInputStream(datas[i]), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
			files = writer.getFiles();

		}

		Assert.assertEquals(datas.length, files.size());

		final Set<String> names = new HashSet<>();
		final List<byte[]> actualDatas = new ArrayList<>();
		for (final File file : files) {
			final FileBlobStoreReader reader = new FileBlobStoreReader(file);
			reader.visitBlobEntries(new BlobEntryVisitor() {
				@Override
				public void visit(final BlobEntry blobEntry, final InputStream contents) throws IOException {
					final String name = blobEntry.getName();
					Assert.assertFalse(names.contains(name));
					names.add(name);
					final byte[] data = new byte[(int)blobEntry.getLength()];
					IOUtils.read(contents, data);
					actualDatas.add(data);
				}
			});
		}

		for (int i = 0; i < datas.length; i++) {
			final byte[] expectedData = datas[i];
			final byte[] actualData = actualDatas.get(i);
			Assert.assertArrayEquals(expectedData, actualData);
		}
	}
}
