package de.mklinger.blobstore;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.mklinger.blobstore.impl.BlobEntryImpl;

public class MultiFileBlobStoreReaderTest {
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private byte[][] datas;
	private List<File> files;

	@Before
	public void setUp() throws IOException {
		datas = new byte[26][];
		for (int i = 0; i < datas.length; i++) {
			datas[i] = Random.getRandomString(100).getBytes(StandardCharsets.UTF_8);
		}

		files = newRotatedBlobStore();
		Assert.assertEquals(datas.length, files.size());
	}

	@Test
	public void visitTest() throws IOException {
		final MultiFileBlobStoreReader reader = new MultiFileBlobStoreReader(files);

		final Set<String> names = new HashSet<>();
		final List<byte[]> actualDatas = new ArrayList<>();
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

		for (int i = 0; i < datas.length; i++) {
			final byte[] expectedData = datas[i];
			final byte[] actualData = actualDatas.get(i);
			Assert.assertArrayEquals(expectedData, actualData);
		}
	}

	@Test
	public void getBlobEntryTest() throws IOException {
		final MultiFileBlobStoreReader reader = new MultiFileBlobStoreReader(files);

		for (int idx = 0; idx < datas.length; idx++) {
			assertEquals(getName(idx), reader.getBlobEntry(getName(idx)).getName());
		}

		assertNull(reader.getBlobEntry("doesnotexist"));
	}

	@Test
	public void getBlobEntryContentsTest() throws IOException {
		final MultiFileBlobStoreReader reader = new MultiFileBlobStoreReader(files);

		for (int idx = 0; idx < datas.length; idx++) {
			assertArrayEquals(datas[idx], IOUtils.toByteArray(reader.getBlobEntryContents(getName(idx))));
		}

		assertNull(reader.getBlobEntryContents("doesnotexist"));
	}

	@Test
	public void getContentsForBlobEntryTest() throws IOException {
		final MultiFileBlobStoreReader reader = new MultiFileBlobStoreReader(files);

		for (int idx = 0; idx < datas.length; idx++) {
			final BlobEntry blobEntry = reader.getBlobEntry(getName(idx));
			assertArrayEquals(datas[idx], IOUtils.toByteArray(reader.getBlobEntryContents(blobEntry)));
		}
	}

	private List<File> newRotatedBlobStore() throws IOException {
		List<File> files;
		final File directory = tmp.newFolder();

		try (RotatingFileBlobStoreWriter writer = RotatingFileBlobStoreWriter.builder()
				.withDirectory(directory)
				.withPrefix("blob")
				.withSuffix(".bin")
				.withMaxEntryCountPerFile(1)
				.build()) {

			for (int i = 0; i < datas.length; i++) {
				writer.addBlobEntry(getName(i), new ByteArrayInputStream(datas[i]), "text/plain", BlobEntryImpl.ENCODING_IDENTITY);
			}
			files = writer.getFiles();
		}
		return files;
	}

	private String getName(int dataIdx) {
		return "Entry" + (char)('A' + (char)dataIdx);
	}
}
